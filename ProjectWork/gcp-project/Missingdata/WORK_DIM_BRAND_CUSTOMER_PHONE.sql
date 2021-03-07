--/*********************************************************************
--* 
--* Script Name: BRAND_CUSTOMER_PHONE table creation and data population
--*
--**********************************************************************/


set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;
!set force true;
--brand_customer_phone_key:md5(concat('JUS',a.individual_id
--                                      ,trim(e.phone_no)
--                                  ))

--drop TABLE ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER_PHONE
--Create table statement
create TABLE IF NOT EXISTS ascena_analytic_mart_dev.WORK_DIM_BRAND_CUSTOMER_PHONE
(
     brand_customer_phone_key CHAR(32)
    ,brand_customer_key  CHAR(32)
    ,brand_cd               string
    ,phone_type_cd  string
    ,create_tms TIMESTAMP
    ,create_source_cd  string
    ,create_job_id  string
    ,phone_preference_cd  string
    ,text_message_preference_cd  string
    ,last_update_tms TIMESTAMP
    ,last_update_source_cd  string
    ,last_update_job_id  string
    ,phone_preference_change_tms TIMESTAMP
    ,text_message_preference_change_tms TIMESTAMP
    ,batch_id  string
)
STORED AS avro
TBLPROPERTIES("avro.output.codec"="snappy")
;  


--insert statement
INSERT OVERWRITE TABLE ascena_analytic_mart_dev.WORK_DIM_BRAND_CUSTOMER_PHONE
select md5(concat('JUS|',coalesce(e.individual_id,'')
                  ,'|' ,coalesce(e.phone_no,'')
                 )) as brand_customer_phone_key
    ,md5(concat('JUS|',coalesce(e.individual_id,''))) as brand_customer_key
    ,'JUS' as brand_cd
    ,e.phone_type_code as phone_type_cd
    ,from_unixtime(UNIX_TIMESTAMP(e.create_date,"MMddyyyy")) as create_tms
    ,e.create_source_code as create_source_cd
    ,e.create_job_id as create_job_id 
    ,e.phone_preference_code as phone_preference_cd
    ,e.text_message_preference_code as text_message_preference_cd
    ,from_unixtime(UNIX_TIMESTAMP(e.last_update_time,"MMddyyyy")) as last_update_tms
    ,e.last_update_source_code as last_update_source_cd
    ,e.last_update_job_id as last_update_job_id
    ,from_unixtime(UNIX_TIMESTAMP(e.phone_preference_change_time,"MMddyyyy")) as phone_preference_change_tms
    ,from_unixtime(UNIX_TIMESTAMP(e.txt_msg_preference_change_time,"MMddyyyy")) as text_message_preference_change_tms
    ,e.batch_id as batch_id
from ASCENA_STAGING_DEV.jus_hst_customer_phones_drh_dedup as e
;

--Primary Key check
--select brand_customer_phone_key from ascena_outbound_dev.DIM_BRAND_CUSTOMER_PHONE_20190104 group by brand_customer_phone_key having count(*)>1;

