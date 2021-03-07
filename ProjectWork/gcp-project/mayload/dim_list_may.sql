--/*********************************************************************
--* 
--* Script Name: DIM_LIST table creation and data population
--*
--**********************************************************************/
--/*
set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.dim_list_may (
LIST_KEY CHAR(32)
, list_id BIGINT
, list_type_cd STRING
, list_desc STRING
, list_dt TIMESTAMP
, list_cd STRING
, list_expiration_dt TIMESTAMP
, division_cd STRING
, list_max_usage_cnt BIGINT
, list_quantity_num BIGINT
, last_update_dt TIMESTAMP
, last_update_job_id BIGINT
, BATCH_ID BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy");


INSERT OVERWRITE TABLE ascena_analytic_mart_dev.dim_list_may
SELECT 
md5(concat('JUS|', coalesce(hl.list_id, ''))) as LIST_KEY
,list_id as list_id
,list_type_code as list_type_cd
,list_description as list_desc     
,from_unixtime(UNIX_TIMESTAMP(list_date,"MM/dd/yyyy HH:mm:ss")) as list_dt
,list_code as list_cd  
,from_unixtime(UNIX_TIMESTAMP(list_expiration_date,"MM/dd/yyyy HH:mm:ss")) as list_expiration_dt
,division_code  as division_cd      
,list_max_usage_cnt   as list_max_usage_cnt
,list_quantity as list_quantity_num    
,from_unixtime(UNIX_TIMESTAMP(last_update_date,"MM/dd/yyyy HH:mm:ss")) as last_update_dt
,last_update_job_id as last_update_job_id  
,batch_id
FROM ascena_staging_dev.jus_hst_lists_dr  AS hl where hl.batch_id='20190501'
UNION
SELECT 
md5(concat('JUS|', coalesce(hcl.list_id, ''))) as LIST_KEY
,hcl.list_id as list_id
,cast(NULL as string) as list_type_cd
,cast(NULL as string) as list_desc 
,cast(NULL as string) as list_dt
,cast(NULL as string) as list_cd 
,cast(NULL as string) as list_expiration_dt
,cast(NULL as string) as division_cd      
,cast(NULL as BIGINT) as list_max_usage_cnt
,cast(NULL as BIGINT) as list_quantity_num  
,cast(NULL as string) as last_update_dt
,cast(NULL as BIGINT) as last_update_job_id 
,cast(NULL as bigint) as BATCH_ID
FROM ASCENA_STAGING_DEV.jus_hst_customer_lists_dr   AS hcl
left join ascena_staging_dev.jus_hst_lists_dr hl
on hcl.list_id = hl.list_id
where hl.list_id is null and hcl.batch_id='20190501'
;