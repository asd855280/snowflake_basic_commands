-- Create table first
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.employees (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
  );

-- Create a file format obejct for stage
CREATE OR REPLACE file format MANAGE_DB.FILE_FORMATS.csv_snowpipe
    type=csv
    field_delimiter=','
    skip_header=1
    null_if=('NULL','null')
    empty_field_as_null=TRUE;

-- Create stage
CREATE OR REPLACE stage MANAGE_DB.external_stages.aws_kc_stage_csv_snowpipe_by_integ
    url = 's3://kc-snowflake/csv_for_snowpipe'
    storage_integration = aws_kc_data
    file_format = MANAGE_DB.FILE_FORMATS.csv_snowpipe;

-- List files in the stage
LIST @MANAGE_DB.external_stages.aws_kc_stage_csv_snowpipe_by_integ;



// Create schema to keep things organized
CREATE OR REPLACE SCHEMA MANAGE_DB.csv_pipes;



// Define pipe
CREATE OR REPLACE pipe MANAGE_DB.csv_pipes.employee_pipe
auto_ingest = TRUE
AS
COPY INTO OUR_FIRST_DB.PUBLIC.employees
FROM @MANAGE_DB.external_stages.aws_kc_stage_csv_snowpipe_by_integ;


// Describe pipe
DESC pipe employee_pipe;
-- value in notification channel column is what we need to set up an S3 notification through SQS


SELECT * FROM OUR_FIRST_DB.PUBLIC.EMPLOYEES;


-- if pipe occurs error
// Snowpipe error message

SELECT * FROM TABLE (INFORMATION_SCHEMA.COPY_HISTORY(
   table_name  =>  'OUR_FIRST_DB.PUBLIC.EMPLOYEES',
   START_TIME =>DATEADD(HOUR,-2,CURRENT_TIMESTAMP())));
