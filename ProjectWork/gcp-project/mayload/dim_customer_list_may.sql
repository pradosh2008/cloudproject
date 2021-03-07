--/*********************************************************************
--* 
--* Script Name: DIM_CUSTOMER_LIST table creation and data population
--*
--**********************************************************************/
--/*
set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.dim_customer_list_may (
CUSTOMER_LIST_KEY CHAR(32)
, brand_customer_key CHAR(32)
, LIST_KEY CHAR(32)
, last_update_dt TIMESTAMP
, last_update_job_id BIGINT
, BATCH_ID BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy");

INSERT OVERWRITE TABLE ascena_analytic_mart_dev.dim_customer_list_may
SELECT 
md5(concat('JUS|', coalesce(hcl.individual_id, ''), '|', coalesce(hcl.list_id, ''))) as CUSTOMER_LIST_KEY
,md5(concat('JUS', coalesce(hcl.individual_id, ''))) as brand_customer_key
,md5(concat('JUS|', coalesce(hcl.list_id, ''))) as LIST_KEY
,from_unixtime(UNIX_TIMESTAMP(hcl.last_update_date,"MM/dd/yyyy HH:mm:ss")) as last_update_dt
,hcl.last_update_job_id as last_update_job_id       
,hcl.batch_id
FROM ASCENA_STAGING_DEV.jus_hst_customer_lists_dr AS hcl where hcl.batch_id='20190501';
