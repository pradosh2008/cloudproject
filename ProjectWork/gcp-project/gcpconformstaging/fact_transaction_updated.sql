CREATE or replace table `p-asna-analytics-002.analytic_mart.fact_transaction`
as
select
transaction_key
,brand_customer_key
,store_key
,transaction_dt
,order_dt
,transaction_num
,register_num
,channel_cd
,transaction_type_cd
,transaction_entry_type_cd
,cashier_id
,store_manager_id
,layaway_num
,multi_brand_transaction_ind
,domestic_order_ind
,address_capture_ind
,email_capture_ind
,return_receipt_ind
,order_num
,transaction_void_ind
,ship_to_store_key
,membership_ind
,plcc_payment_reference_nbr
,plcc_payment_line_object_cd
,plcc_payment_tm
,plcc_payment_amt
,p2e_ind
,edl_create_tms
,edl_create_job_nam
,edl_last_update_tms
,edl_last_update_job_nam
,brand_cd
from (select
t.transaction_key as transaction_key
,case when brand_customer_key is null then '0' else brand_customer_key end as brand_customer_key
,store_key
,transaction_dt
,case when order_receive_dt is null then transaction_dt else order_receive_dt end as order_dt
,transaction_num
,register_num
,channel_cd
,transaction_type_cd
,transaction_entry_type_cd
,cashier_id
,store_manager_id
,layaway_num
,multi_brand_transaction_ind
,domestic_order_ind
,address_capture_ind
,email_capture_ind
,return_receipt_ind
,order_num
,transaction_void_ind
,ship_to_store_key
,membership_ind
,plcc_payment_reference_nbr
,plcc_payment_line_object_cd
,plcc_payment_tm
,plcc_payment_amt
,p2e_ind
,t.edl_create_tms as edl_create_tms
,t.edl_create_job_nam as edl_create_job_nam
,t.edl_last_update_tms as edl_last_update_tms
,t.edl_last_update_job_nam as edl_last_update_job_nam
,t.brand_cd as brand_cd
,row_number() over (
partition by t.transaction_key 
order by 
                              CASE c.brand_customer_transaction_role_cd 
                                             WHEN 'BUYER' THEN 1 
                                             WHEN 'PAYER' THEN 2 
                                             ELSE 1 
                              END ) as rn
FROM `p-asna-analytics-002.edl_conform.transaction` as t
LEFT OUTER JOIN `p-asna-analytics-002.edl_conform.brand_customer_transaction_xref` as c
ON t.transaction_key = c.transaction_key ) as a where rn=1
;