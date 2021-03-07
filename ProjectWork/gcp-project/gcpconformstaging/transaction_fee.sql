set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.transaction_fee (
transaction_key	string
,transaction_fee_line_num	int
,fee_cd	int
,fee_amt	float
,discount_amt	float
,non_merchandise_tax_type_cd	string
,non_merchandise_prorated_tax_amt	float
,non_merchandise_tax_amt	float
,edl_create_tms	timestamp
,edl_create_job_nam	string
,edl_last_update_tms	timestamp
,edl_last_update_job_nam	string
,brand_cd	string             
 )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


insert overwrite table ascena_analytic_mart.transaction_fee
select  transaction_key
,transaction_fee_line_num
,fee_cd
,fee_amt
,discount_amt
,non_merchandise_tax_type_cd
,non_merchandise_prorated_tax_amt
,non_merchandise_tax_amt
,edl_create_tms
,edl_create_job_nam
,edl_last_update_tms
,edl_last_update_job_nam
,brand_cd                
from  ascena_conform.transaction_fee
where edl_last_update_tms > date_add(current_date,-3)
;