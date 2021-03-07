--/*********************************************************************
--* 
--* Script Name: BRAND_CUSTOMER_EMAIL_DIVISION table creation and data population
--*
--**********************************************************************/

set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
set mapred.output.compression.type=BLOCK;

--brand_customer_email_division_key: md5(concat('JUS',e.individual_id
--                                      ,lower(trim(e.email_address ))
--                                  ))

--Create table statement
create TABLE IF NOT EXISTS ascena_analytic_mart_dev.dim_brand_customer_email_division_may 
(
     brand_customer_email_division_key CHAR(32)
	,brand_customer_email_key CHAR(32)
    ,brand_customer_key  CHAR(32)
	,division_cd string
	,email_address_id string
	,household_id string
	,latest_hard_bounce_dt TIMESTAMP
	,bounced_ind TINYINT
	,complaint_ind TINYINT
	,soft_bounce_cnt string
	,email_preference_cd string
	,invalid_ind TINYINT
	,last_email_send_dt TIMESTAMP
	,last_email_open_dt TIMESTAMP
	,last_email_click_dt TIMESTAMP
	,email_sends_1_mo_cnt INT     
	,email_sends_2_mo_cnt INT     
	,email_sends_3_mo_cnt INT     
	,email_sends_4_6_mo_cnt INT   
	,email_sends_7_12_mo_cnt INT  
	,email_sends_13_24_mo_cnt INT 
	,email_sends_25_36_mo_cnt INT 
	,email_opens_1_mo_cnt INT     
	,email_opens_2_mo_cnt INT     
	,email_opens_3_mo_cnt INT     
	,email_opens_4_6_mo_cnt INT   
	,email_opens_7_12_mo_cnt INT  
	,email_clicks_1_mo_cnt INT    
	,email_clicks_2_mo_cnt INT    
	,email_clicks_3_mo_cnt INT    
	,email_clicks_4_6_mo_cnt INT  
	,email_clicks_7_12_mo_cnt INT
	,max_unsub_pref_dt TIMESTAMP
    ,batch_id  string
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

INSERT OVERWRITE TABLE ascena_analytic_mart_dev.dim_brand_customer_email_division_may
select
 brand_customer_email_division_key
,brand_customer_email_key
,brand_customer_key
,division_cd
,email_address_id
,household_id
,latest_hard_bounce_dt
,bounced_ind
,complaint_ind
,soft_bounce_cnt
,email_preference_cd
,invalid_ind
,last_email_send_dt 
,last_email_open_dt 
,last_email_click_dt
,email_sends_1_mo_cnt     
,email_sends_2_mo_cnt     
,email_sends_3_mo_cnt     
,email_sends_4_6_mo_cnt   
,email_sends_7_12_mo_cnt  
,email_sends_13_24_mo_cnt 
,email_sends_25_36_mo_cnt 
,email_opens_1_mo_cnt     
,email_opens_2_mo_cnt     
,email_opens_3_mo_cnt     
,email_opens_4_6_mo_cnt   
,email_opens_7_12_mo_cnt  
,email_clicks_1_mo_cnt    
,email_clicks_2_mo_cnt    
,email_clicks_3_mo_cnt    
,email_clicks_4_6_mo_cnt  
,email_clicks_7_12_mo_cnt 
,max_unsub_pref_dt 
,batch_id  
from (select 
         md5(concat(coalesce(upper(trim(e.division_code )),''),'|', coalesce(e.email_address_id,''))) as brand_customer_email_division_key
        ,md5(concat('JUS|',coalesce(e.individual_id,''),'|', coalesce(upper(trim(e.email_address )),''))) as brand_customer_email_key
        ,md5(concat('JUS|',coalesce(e.individual_id,''))) as brand_customer_key
		,e.division_code as division_cd
        ,e.email_address_id
		,e.household_id
		,from_unixtime(UNIX_TIMESTAMP(e.latest_hard_bounce_date,"MM/dd/yyyy HH:mm:ss")) as latest_hard_bounce_dt
        ,CASE WHEN e.bounced_flag ='Y' THEN 1 WHEN e.bounced_flag='N' THEN 0 ELSE 0 END as bounced_ind
        ,CASE WHEN e.complaint_flag ='Y' THEN 1 WHEN e.complaint_flag ='N' THEN 0 ELSE 0 END as complaint_ind
		,e.soft_bounce_count as soft_bounce_cnt
		,e.email_preference_code as email_preference_cd
		,CASE WHEN e.invalid_flag ='Y' THEN 1 WHEN e.invalid_flag ='N' THEN 0 ELSE 0 END as invalid_ind
		,from_unixtime(UNIX_TIMESTAMP(e.last_email_send_date,"MM/dd/yyyy HH:mm:ss")) as last_email_send_dt
		,from_unixtime(UNIX_TIMESTAMP(e.last_email_open_date,"MM/dd/yyyy HH:mm:ss")) as last_email_open_dt
		,from_unixtime(UNIX_TIMESTAMP(e.last_email_click_date,"MM/dd/yyyy HH:mm:ss")) as last_email_click_dt
		,e.email_sends_1_mo as email_sends_1_mo_cnt
	    ,e.email_sends_2_mo as email_sends_2_mo_cnt 
        ,e.email_sends_3_mo as email_sends_3_mo_cnt  
        ,e.email_sends_4_6_mo as email_sends_4_6_mo_cnt
        ,e.email_sends_7_12_mo as email_sends_7_12_mo_cnt
        ,e.email_sends_13_24_mo as email_sends_13_24_mo_cnt
        ,e.email_sends_25_36_mo as email_sends_25_36_mo_cnt
        ,e.email_opens_1_mo as email_opens_1_mo_cnt  
        ,e.email_opens_2_mo as email_opens_2_mo_cnt  
        ,e.email_opens_3_mo as email_opens_3_mo_cnt  
        ,e.email_opens_4_6_mo as email_opens_4_6_mo_cnt
        ,e.email_opens_7_12_mo as email_opens_7_12_mo_cnt
        ,e.email_clicks_1_mo as email_clicks_1_mo_cnt 
        ,e.email_clicks_2_mo as email_clicks_2_mo_cnt  
        ,e.email_clicks_3_mo as   email_clicks_3_mo_cnt
        ,e.email_clicks_4_6_mo as email_clicks_4_6_mo_cnt
        ,e.email_clicks_7_12_mo as email_clicks_7_12_mo_cnt
        ,from_unixtime(UNIX_TIMESTAMP(e.max_unsub_pref_date,"MM/dd/yyyy HH:mm:ss")) as max_unsub_pref_dt
        ,e.batch_id as batch_id

    from ascena_staging_dev.jus_hst_email_divisions_dr as e where e.batch_id='20190501'
) as main;


