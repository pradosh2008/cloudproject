set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.transaction_tender (
transaction_key	string
,tender_line_num	int
,tender_amt	float
,tender_expiration_dt	date
,tender_cd	string
,tender_reference_num	string
,tender_void_ind	tinyint
,deferred_billing_dt	date
,paypal_order_id	string
,tender_salesperson_key	string
,authorization_id	string
,authorize_key_ind	tinyint
,deferred_billing_plan_id	string
,open_charge_ind	tinyint
,charge_application_status_cd	string
,state_cd	string
,record_create_tm	timestamp
,edl_create_tms	timestamp
,edl_create_job_nam	string
,edl_last_update_tms	timestamp
,edl_last_update_job_nam	string
,brand_cd	string               
 )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


insert overwrite table ascena_analytic_mart.transaction_tender
select  transaction_key
,tender_line_num
,tender_amt
,tender_expiration_dt
,tender_cd
,tender_reference_num
,tender_void_ind
,deferred_billing_dt
,paypal_order_id
,tender_salesperson_key
,authorization_id
,authorize_key_ind
,deferred_billing_plan_id
,open_charge_ind
,charge_application_status_cd
,state_cd
,record_create_tm
,edl_create_tms
,edl_create_job_nam
,edl_last_update_tms
,edl_last_update_job_nam
,brand_cd                  
from ascena_conform.transaction_tender
where edl_last_update_tms > date_add(current_date,-3)
;