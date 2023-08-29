SELECT *
  FROM <database>.<schema>.<TABLE_NAME>
BEFORE (statement => '<query_id>')

-- Be aware that when we drop or recreate a table,
-- we lost the metadata for time travel. Therefore, it is important to avoid dropping table if not neccesary

-- However, in Snowflake, table, schema and database can be UNDROP after accidentally dropped.

-- In case that if we accitentally dropped a table implicitly by executing CREATE OR REPLACE command
-- , we won't be able to directly execute UNDROP command.
-- Becuase two tables with same name can not be exist at the same time,
-- therefore, we can rename the corrupted table first, then execute UNDROP command to restore the implicitly dropped table.


-- Show table retention time
SHOW PARAMETERS like '%DATA_RETENTION_TIME_IN_DAYS%' in table <database>.<schema>.<TABLE_NAME>;

SHOW TABLES LIKE '%ORDERS%';



-- Restoring table from time travel
CREATE OR REPLACE TABLE <database>.<schema>.<new_restored_table> as
SELECT *
  FROM O<database>.<schema>.<noriginal_table>
BEFORE (statement => '019b9ef0-0500-8473-0043-4d830007309a')


-- time travel cost example 
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE ORDER BY USAGE_DATE DESC;


SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

-- Query time travel storage

SELECT 	ID,
		TABLE_NAME,
		TABLE_SCHEMA,
        TABLE_CATALOG,
		ACTIVE_BYTES / (1024*1024*1024) AS STORAGE_USED_GB,
		TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE_USED_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
ORDER BY STORAGE_USED_GB DESC,TIME_TRAVEL_STORAGE_USED_GB DESC;



------------------------------------
SELECT *
  FROM OUR_FIRST_DB.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS;
