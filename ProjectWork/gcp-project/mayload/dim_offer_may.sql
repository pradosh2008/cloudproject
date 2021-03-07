--/*********************************************************************
--* 
--* Script Name: DIM_OFFER table creation and data population
--*
--**********************************************************************/

set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.dim_offer_may
(
     OFFER_KEY CHAR(32)
    ,OFFER_CODE_ID BIGINT 
    ,OFFER_CD STRING
    ,OFFER_START_DT DATE
    ,OFFER_END_DT DATE
    ,OFFER_TYPE_CD STRING
    ,DISCOUNT_TYPE_CD STRING
    ,DISCOUNT_VALUE_NUM FLOAT
    ,OFFER_DESC STRING
    ,OFFER_STATUS_CD STRING
    ,OFFER_CATEGORY_CD STRING
    ,HARD_START_IND TINYINT
    ,HARD_END_IND TINYINT
    ,CREATE_DT DATE
    ,CREATE_JOB_ID BIGINT
    ,LAST_UPDATE_DT DATE
    ,LAST_UPDATE_JOB_ID BIGINT
    ,BATCH_ID BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


INSERT OVERWRITE TABLE ascena_analytic_mart_dev.dim_offer_may
SELECT 
 md5(concat('JUS|',COALESCE(jho.offer_code_id, CAST(0 AS BIGINT)), '|', coalesce(upper(trim(jho.offer_code)), ''))) as OFFER_KEY
,jho.offer_code_id as offer_code_id
,jho.offer_code as OFFER_CD
,to_date(from_unixtime(UNIX_TIMESTAMP(jho.offer_start_date,"MM/dd/yyyy"))) as OFFER_START_DT
,to_date(from_unixtime(UNIX_TIMESTAMP(jho.offer_end_date,"MM/dd/yyyy"))) as OFFER_END_DT
,jho.offer_type_code as OFFER_TYPE_CD
,jho.discount_type_code as DISCOUNT_TYPE_CD
,jho.discount_amount as DISCOUNT_VALUE_NUM
,jho.offer_desc as OFFER_DESC
,jho.offer_status_code as OFFER_STATUS_CD
,jho.offer_category as OFFER_CATEGORY_CD
,
    CASE 
        WHEN jho.hard_start_flag ='Y' THEN 1 
        WHEN jho.hard_start_flag ='N' THEN 0  
        ELSE 0 
    END as HARD_START_IND
,
    CASE 
        WHEN jho.hard_end_flag ='Y' THEN 1 
        WHEN jho.hard_end_flag ='N' THEN 0 
        ELSE 0 
   END as HARD_END_IND
,to_date(from_unixtime(UNIX_TIMESTAMP(jho.create_date,"MM/dd/yyyy"))) as CREATE_DT
,jho.create_job_id as CREATE_JOB_ID
,to_date(from_unixtime(UNIX_TIMESTAMP(jho.last_update_date,"MM/dd/yyyy"))) as LAST_UPDATE_DT
,jho.last_update_job_id as LAST_UPDATE_JOB_ID
,jho.batch_id as BATCH_ID
FROM ascena_staging_dev.jus_hst_offer_codes_dr as jho where jho.batch_id='20190501'
UNION
SELECT 
md5(concat('JUS|','MERCH','|', '000')) as OFFER_KEY
,cast(NULL as BIGINT) as offer_code_id
,'000' as OFFER_CD
,cast(NULL as DATE) as OFFER_START_DT	
,cast(NULL as DATE) as OFFER_END_DT	
,'MERCH' as OFFER_TYPE_CD	
,cast(NULL as string) as DISCOUNT_TYPE_CD	
,cast(NULL as float) as DISCOUNT_VALUE_NUM
,cast(NULL as string) as OFFER_DESC	
,cast(NULL as string) as OFFER_STATUS_CD	
,cast(NULL as string) as OFFER_CATEGORY_CD
,cast(NULL as tinyint) as HARD_START_IND
,cast(NULL as tinyint) as HARD_END_IND	
,cast(NULL as DATE) as CREATE_DT	
,cast(NULL as bigint) as CREATE_JOB_ID	
,cast(NULL as DATE) as LAST_UPDATE_DT	
,cast(NULL as bigint) as LAST_UPDATE_JOB_ID	
,cast(NULL as bigint) as BATCH_ID
UNION
SELECT
md5(concat('JUS|','OTHER','|', '000')) as OFFER_KEY
,cast(NULL as BIGINT) as offer_code_id
,'000' as OFFER_CD
,cast(NULL as DATE) as OFFER_START_DT	
,cast(NULL as DATE) as OFFER_END_DT	
,'OTHER' as OFFER_TYPE_CD	
,cast(NULL as string) as DISCOUNT_TYPE_CD	
,cast(NULL as float) as DISCOUNT_VALUE_NUM
,cast(NULL as string) as OFFER_DESC	
,cast(NULL as string) as OFFER_STATUS_CD	
,cast(NULL as string) as OFFER_CATEGORY_CD
,cast(NULL as tinyint) as HARD_START_IND
,cast(NULL as tinyint) as HARD_END_IND	
,cast(NULL as DATE) as CREATE_DT	
,cast(NULL as bigint) as CREATE_JOB_ID	
,cast(NULL as DATE) as LAST_UPDATE_DT	
,cast(NULL as bigint) as LAST_UPDATE_JOB_ID	
,cast(NULL as bigint) as BATCH_ID
UNION
SELECT	
md5(concat('JUS|',COALESCE(JHTD.offer_code_id, CAST(0 AS BIGINT)), '|', coalesce(upper(trim(JHTD.offer_code)), ''))) as OFFER_KEY
,JHTD.offer_code_id as offer_code_id
,JHTD.offer_code as OFFER_CD
,cast(NULL as DATE) as OFFER_START_DT	
,cast(NULL as DATE) as OFFER_END_DT
,JHTD.offer_type_code as OFFER_TYPE_CD
,cast(NULL as string) as DISCOUNT_TYPE_CD	
,cast(NULL as float) as DISCOUNT_VALUE_NUM
,cast(NULL as string) as OFFER_DESC	
,cast(NULL as string) as OFFER_STATUS_CD	
,cast(NULL as string) as OFFER_CATEGORY_CD
,cast(NULL as tinyint) as HARD_START_IND
,cast(NULL as tinyint) as HARD_END_IND	
,cast(NULL as DATE) as CREATE_DT	
,cast(NULL as bigint) as CREATE_JOB_ID	
,cast(NULL as DATE) as LAST_UPDATE_DT	
,cast(NULL as bigint) as LAST_UPDATE_JOB_ID	
,cast(NULL as bigint) as BATCH_ID
from ascena_staging_dev.jus_hst_transaction_discounts_dr as JHTD 
LEFT OUTER JOIN ascena_staging_dev.jus_hst_offer_codes_dr as JHO
	on JHTD.offer_code_id=JHO.offer_code_id 
where JHO.offer_code_id IS NULL  
and JHTD.offer_type_code IN ('MARK')
and JHTD.batch_id='20190501'
group by md5(concat('JUS|',COALESCE(JHTD.offer_code_id, CAST(0 AS BIGINT)),'|', coalesce(upper(trim(JHTD.offer_code)), '')))
		,JHTD.offer_code_id
		,JHTD.OFFER_CODE
		,JHTD.OFFER_TYPE_CODE
;