--/*********************************************************************
--* 
--* Script Name: BRAND_CUSTOMER_EMAIL table creation and data population
--*
--**********************************************************************/

set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


--brand_customer_email_key: md5(concat('JUS',e.individual_id
--                                      ,lower(trim(e.email_address ))
--                                  ))

--Create table statement
drop table ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER_EMAIL;
create TABLE IF NOT EXISTS ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER_EMAIL 
(
     brand_customer_email_key CHAR(32)
    ,brand_customer_key  CHAR(32)
    ,brand_cd               string
    ,complaint_ind TINYINT
    ,bounced_ind TINYINT
    ,coppa_ind TINYINT
    ,create_source_cd  string
    ,fresh_addr_comment_str  string
    ,fresh_addr_hygiene_cd  string
    ,fresh_addr_update_tms TIMESTAMP
    ,invalid_ind TINYINT
    ,last_update_source_cd  string
    ,soft_bounce_cnt INT
    ,email_preference_change_tms TIMESTAMP
    ,create_tms TIMESTAMP
    ,create_job_id  string
    ,last_update_tms TIMESTAMP
    ,last_update_job_id  string
    ,email_preference_cd  string
    ,email_address_id  DECIMAL(10,0)
    ,subscr_status_justice  string
    ,subscr_status_justice_dt DATE
    ,subscr_status_bro  string
    ,subscr_status_bro_dt DATE
    ,subscr_status_justice_can  string
    ,subscr_status_justice_can_dt DATE
    ,et_rejected  string
    ,batch_id  string
)
STORED AS avro
TBLPROPERTIES("avro.output.codec"="snappy")
;

INSERT OVERWRITE TABLE ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER_EMAIL 
select
 brand_customer_email_key
,brand_customer_key
,brand_cd
,complaint_ind
,bounced_ind
,coppa_ind
,create_source_cd
,fresh_addr_comment_str
,fresh_addr_hygiene_cd
,fresh_addr_update_tms
,invalid_ind
,last_update_source_cd
,soft_bounce_cnt
,email_preference_change_tms
,create_tms
,create_job_id
,last_update_tms
,last_update_job_id
,email_preference_cd
,email_address_id 
,subscr_status_justice 
,subscr_status_justice_dt 
,subscr_status_bro
,subscr_status_bro_dt 
,subscr_status_justice_can 
,subscr_status_justice_can_dt
,et_rejected 
,batch_id
from (select md5(concat('JUS|',coalesce(e.individual_id,''),'|', coalesce(upper(trim(e.email_address )),''))) as brand_customer_email_key
        ,md5(concat('JUS|',coalesce(e.individual_id,''))) as brand_customer_key
        ,'JUS' as brand_cd
        ,CASE WHEN e.complaint_flag ='Y' THEN 1 WHEN e.complaint_flag='N' THEN 0 ELSE 0 END as complaint_ind
        ,CASE WHEN e.bounced_flag ='Y' THEN 1 WHEN e.bounced_flag ='N' THEN 0 ELSE 0 END as bounced_ind
        ,CASE WHEN e.coppa_flag ='Y' THEN 1 WHEN e.coppa_flag ='N' THEN 0 ELSE 0 END as coppa_ind
        ,e.create_source_code  as create_source_cd
        ,e.fra_comment         as fresh_addr_comment_str
        ,e.fra_hygiene_cd      as fresh_addr_hygiene_cd
        ,from_unixtime(UNIX_TIMESTAMP(e.fra_update_date,"MM/dd/yyyy HH:mm:ss")) as fresh_addr_update_tms
        ,CASE WHEN e.invalid_flag ='Y' THEN 1 WHEN e.invalid_flag ='N' THEN 0  ELSE 0 END as invalid_ind
        ,e.last_update_source_code     as last_update_source_cd
        ,e.soft_bounce_count           as soft_bounce_cnt
        ,from_unixtime(UNIX_TIMESTAMP(e.email_preference_change_time,"MM/dd/yyyy HH:mm:ss")) as email_preference_change_tms
        ,from_unixtime(UNIX_TIMESTAMP(e.create_date,"MM/dd/yyyy HH:mm:ss")) as create_tms
        ,e.create_job_id as create_job_id
        ,from_unixtime(UNIX_TIMESTAMP(e.last_update_time,"MM/dd/yyyy HH:mm:ss")) as last_update_tms
        ,e.last_update_job_id as last_update_job_id
        ,e.email_preference_code as email_preference_cd
        ,e.email_address_id as email_address_id
        ,e.subscr_status_justice as subscr_status_justice
        ,cast(from_unixtime(UNIX_TIMESTAMP(e.subscr_status_justice_date,"yyyyMMdd")) as date) as subscr_status_justice_dt
        ,e.subscr_status_bro as subscr_status_bro
        ,cast(from_unixtime(UNIX_TIMESTAMP(e.subscr_status_bro_date,"yyyyMMdd")) as date) as subscr_status_bro_dt
        ,e.subscr_status_justice_can as subscr_status_justice_can
        ,cast(from_unixtime(UNIX_TIMESTAMP(e.subscr_status_justice_can_date,"yyyyMMdd")) as date) as subscr_status_justice_can_dt
        ,e.et_rejected as et_rejected
        ,e.batch_id as batch_id
        ,row_number() over(partition by e.individual_id, e.email_address 
                           order by from_unixtime(UNIX_TIMESTAMP(e.last_update_time,"MM/dd/yyyy HH:mm:ss")) desc
                        )                                                               as rn
    from ASCENA_STAGING_DEV.jus_hst_customer_email_addresses_drc as e
) as m
where rn=1
;


--Primary Key check
--select brand_customer_email_key,count(*) from ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER_EMAIL group by brand_customer_email_key having count(*) >1
