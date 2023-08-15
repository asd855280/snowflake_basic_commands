-- Create S3 Bucket
-- Create Role with policies that allow access to S3 bucket
-- Then use the Role ARN for following command

-- First, create a storage instegration object
CREATE STORAGE INTEGRATION aws_kc_data
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::250191212473:role/Snowflake'
  STORAGE_ALLOWED_LOCATIONS = ('s3://kc-snowflake');

-- Look for the STORAGE_AWS_ROLE_ARN and STORAGE_AWS_EXTERNAL_ID
-- Use the these two values  to update AWS Role information
DESC INTEGRATION aws_kc_data;

-- After creating the storage integration object with permission, we can now use the object to create stage
CREATE OR REPLACE STAGE aws_kc_stage_by_integration
    STORAGE_INTEGRATION = aws_kc_data
    URL = 's3://kc-snowflake/';

-- List files in the stage(S3 bucket)
LIST @aws_kc_stage_by_integration;

TRUNCATE TABLE EXERCISE_DB.PUBLIC.KC_CUSTOMERS;

-- Load file into table from stage
COPY INTO EXERCISE_DB.PUBLIC.KC_CUSTOMERS (customer_id,customer_name,age)
    FROM (
        SELECT s.$1,
               s.$2,
               s.$3
          FROM @MANAGE_DB.EXTERNAL_STAGES.AWS_KC_STAGE_BY_INTEGRATION s
    )
    file_format=MANAGE_DB.FILE_FORMATS.CSV_WITH_HEADER
    files=('kc_customers.csv');


SELECT * FROM EXERCISE_DB.PUBLIC.KC_CUSTOMERS;


INSERT INTO EXERCISE_DB.PUBLIC.KC_CUSTOMERS
(customer_id,customer_name,age)
VALUES(77,'KC',35);
