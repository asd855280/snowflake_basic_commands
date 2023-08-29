-- Create stream testing database
CREATE OR REPLACE TRANSIENT DATABASE STREAMS_DB;

-- Create example original table
CREATE OR REPLACE TABLE sales_original(
  id varchar,
  product varchar,
  price varchar,
  amount varchar,
  store_id varchar
  );


-- Create table for lookup(Join) when apply stream data processing
CREATE OR REPLACE TABLE store_table(
  store_id number,
  location varchar,
  employees number
  );

INSERT INTO STORE_TABLE VALUES(1,'Chicago',33);
INSERT INTO STORE_TABLE VALUES(2,'London',12);

SELECT * FROM STREAMS_DB.PUBLIC.STORE_TABLE;

-- CREATE a STREAM object
CREATE OR REPLACE STREAM sales_stream ON TABLE STREAMS_DB.PUBLIC.SALES_ORIGINAL;

SHOW STREAMS;
DESC STREAM sales_stream;

-- Get changes on data using stream
-- When consuming data fron stream table by using INSERTS or MERGE statement to apply changes to
-- target tables, the data in stream object will be removed after successfully consumed.
SELECT * FROM sales_stream;


-- CREATE final target table for consuming stream(CDC) data
CREATE OR REPLACE TABLE sales_final_table(
  id int,
  product varchar,
  price number,
  amount int,
  store_id int,
  location varchar,
  employees int
  );

-- INSERT data into original table to create some CDC data
INSERT INTO sales_original
    values
        (1,'Banana',1.99,1,1),
        (2,'Lemon',0.99,1,1),
        (3,'Apple',1.79,1,2),
        (4,'Orange Juice',1.89,1,2),
        (5,'Cereals',5.98,2,1);

SELECT *
  FROM sales_original;

-- UPDATE data on original table
UPDATE sales_original
SET product = 'Cookies'
WHERE id = 5;

-- DELETE data from original tble
DELETE FROM sales_original
WHERE id = 4;

-- PROCESS Stream(CDC) data into target table
MERGE INTO SALES_FINAL_TABLE F      -- Target table to merge changes from source table
USING ( SELECT STRE.*,ST.location,ST.employees
        FROM SALES_STREAM STRE
        JOIN STORE_TABLE ST
        ON STRE.store_id = ST.store_id
       ) S
ON F.id=S.id
WHEN matched                        -- DELETE condition
    AND S.METADATA$ACTION ='DELETE'
    AND S.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE
WHEN matched                        -- UPDATE condition
    AND S.METADATA$ACTION ='INSERT'
    AND S.METADATA$ISUPDATE  = 'TRUE'
    THEN UPDATE
    SET f.product = s.product,
        f.price = s.price,
        f.amount= s.amount,
        f.store_id=s.store_id
WHEN NOT matched
    AND S.METADATA$ACTION ='INSERT'
    THEN INSERT
    (id,product,price,store_id,amount,employees,location)
    VALUES
    (s.id, s.product,s.price,s.store_id,s.amount,s.employees,s.location);


-- Check target table
SELECT *
  FROM STREAMS_DB.PUBLIC.sales_final_table;

-- Check if Stream object has beem consumed
SELECT * FROM sales_stream;
