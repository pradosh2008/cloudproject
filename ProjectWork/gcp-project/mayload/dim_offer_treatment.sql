--/*********************************************************************
--* 
--* Script Name: DIM_OFFER_TREATMENT table creation and data population
--* Changed the Offer Key logic-29/03
--**********************************************************************/
--/*
--md5(concat('JUS|', t.treatment_code)) as TREATMENT_KEY

set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;
!set force true;

drop TABLE ascena_analytic_mart_dev.DIM_OFFER_TREATMENT ;

CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.DIM_OFFER_TREATMENT (
TREATMENT_KEY CHAR(32)
, OFFER_KEY CHAR(32)
, OFFER_CODE_ID BIGINT
, treatment_offer_code STRING 
, TREATMENT_CD STRING
, TREATMENT_TYPE_DES STRING
, TREATMENT_DES STRING
, ACTIVE_IND INT
, TREATMENT_offer_start_dt TIMESTAMP
, TREATMENT_offer_end_dt TIMESTAMP
, TREATMENT_offer_type_cd STRING
, TREATMENT_offer_des STRING
, TREATMENT_offer_status_cd STRING
, TREATMENT_offer_category_des STRING
, last_update_dt TIMESTAMP
, last_update_user_id STRING
, batch_id BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


INSERT OVERWRITE TABLE ascena_analytic_mart_dev.DIM_OFFER_TREATMENT
SELECT md5(concat('JUS|', coalesce(upper(trim(t.treatment_code)), ''))) as TREATMENT_KEY
, md5(concat('JUS|',COALESCE(t.offer_code_id, CAST(0 AS BIGINT)),'|',COALESCE(upper(trim(t.treatment_offer_code)),CAST(0 AS STRING)))) as OFFER_KEY
, t.offer_code_id as OFFER_CODE_ID
, t.treatment_offer_code as treatment_offer_code
, t.treatment_code as TREATMENT_CD
, t.treatment_type as TREATMENT_TYPE_DES
, t.treatment_description as TREATMENT_DES
, CASE t.active_flag WHEN 'Y' THEN 1 ELSE 0 END as ACTIVE_IND	   
, from_unixtime(UNIX_TIMESTAMP(t.offer_start_date,"MM/dd/yyyy HH:mm:ss")) as TREATMENT_offer_start_dt
, from_unixtime(UNIX_TIMESTAMP(t.offer_end_date,"MM/dd/yyyy HH:mm:ss")) as TREATMENT_offer_end_dt
, t.offer_type_code as TREATMENT_offer_type_cd
, t.offer_desc as TREATMENT_offer_des
, t.offer_status_code as TREATMENT_offer_status_cd
, t.offer_category as TREATMENT_offer_category_des
, from_unixtime(UNIX_TIMESTAMP(t.last_update_date,"MM/dd/yyyy HH:mm:ss")) as last_update_dt
, t.last_update_user_id as last_update_user_id
, t.batch_id as batch_id
FROM ascena_staging_dev.jus_hst_treatments_drc as t;
