set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.transaction_discount (
transaction_key	string
,transaction_line_num	int
,discount_line_num	int
,coupon_cd	string
,discount_amt	float
,discount_reason_cd	string
,discount_sub_reason_cd	string
,point_of_sale_event_cd	string
,discount_type_cd	string
,deal_cd	string
,discount_method_cd	string
,edl_create_tms	timestamp
,edl_create_job_nam	string
,edl_last_update_tms	timestamp
,edl_last_update_job_nam	string
,brand_cd	string               
 )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


insert overwrite table ascena_analytic_mart.transaction_discount
select  transaction_key
,transaction_line_num
,discount_line_num
,coupon_cd
,discount_amt
,discount_reason_cd
,discount_sub_reason_cd
,point_of_sale_event_cd
,discount_type_cd
,deal_cd
,discount_method_cd
,edl_create_tms
,edl_create_job_nam
,edl_last_update_tms
,edl_last_update_job_nam
,brand_cd                
from ascena_conform.transaction_discount
where edl_last_update_tms > date_add(current_date,-3)
;