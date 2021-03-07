create or replace table analytic_mart.fact_transaction_detail_costamt_from_29032019_04042020 as
select
transaction_key,
transaction_line_num,
transaction_dt,
order_dt,
store_key,
brand_customer_key,
item_key,
return_ind,
original_transaction_header_key,
original_transaction_store_key,
original_transaction_dt,
original_transaction_line_num,
line_action_cd,
line_object_type_cd,
line_object_cd,
line_void_ind,
scanned_ind,
gift_card_ind,
taxable_ind,
style_locator_ind,
permanent_markdown_type_cd,
return_reason_cd,
return_disposition_cd,
return_reason_desc,
return_to_dc_ind,
layaway_ind,
order_ind,
ROUND(CAST(sku_cost_amt AS NUMERIC), 2) sku_cost_amt,
sku_quantity_num,
ROUND(CAST(sku_retail_amt AS NUMERIC), 2) sku_retail_amt,
ROUND(CAST(markdown_amt AS NUMERIC), 2) markdown_amt,
ROUND(CAST(customer_paid_tax_amt AS NUMERIC), 2) customer_paid_tax_amt,
ROUND(CAST(company_owed_tax_amt AS NUMERIC), 2) as company_owed_tax_amt,
ROUND(CAST(commision_amt AS NUMERIC), 2) as commision_amt,
ROUND(CAST(current_retail_amt AS NUMERIC), 2) as current_retail_amt,
ROUND(CAST(original_unit_retail_amt AS NUMERIC), 2) as original_unit_retail_amt,
primary_salesperson_id,
secondary_salesperson_id,
original_primary_salesperson_id,
original_secondary_salesperson_id,
upc_store_key,
upc_num,
size_cd,
cross_brand_store_key,
overseas_item_ind,
edl_create_tms,
edl_create_job_nam,
edl_last_update_tms,
edl_last_update_job_nam,
brand_cd 
from 
(select 
TD.transaction_key,
TD.transaction_line_num,
TM.transaction_dt,
case when tm.order_receive_dt is null then tm.transaction_dt else tm.order_receive_dt end as order_dt,
TM.store_key,
CASE WHEN bc.brand_customer_key IS NULL THEN '0' else bc.brand_customer_key end as brand_customer_key,
line_action_cd,
line_object_type_cd,
line_object_cd,
line_void_ind,
item_key,
CASE WHEN original_transaction_header_key is NULL THEN TD.transaction_key else original_transaction_header_key end as original_transaction_header_key,
CASE WHEN ST.store_key is NULL THEN TM.store_key else ST.store_key end AS original_transaction_store_key,
CASE WHEN original_transaction_dt is NULL THEN TM.transaction_dt else original_transaction_dt end as original_transaction_dt,
CASE WHEN original_transaction_line_num is NULL THEN TD.transaction_line_num else original_transaction_line_num end as original_transaction_line_num,
tdh.transaction_line_cst as sku_cost_amt,
sku_quantity_num,
sku_retail_amt,
permanent_markdown_type_cd,
markdown_amt,
customer_paid_tax_amt,
company_owed_tax_amt,
return_reason_cd,
commision_amt,
current_retail_amt,
original_unit_retail_amt,
scanned_ind,
gift_card_ind,
taxable_ind,
style_locator_ind,
primary_salesperson_id,
secondary_salesperson_id,
original_primary_salesperson_id,
original_secondary_salesperson_id,
return_ind,
return_disposition_cd,
return_reason_desc,
return_to_dc_ind,
layaway_ind,
order_ind,
upc_store_key,
upc_num,
size_cd,
cross_brand_store_key,
overseas_item_ind,
TD.edl_create_tms,
TD.edl_create_job_nam,
TD.edl_last_update_tms,
TD.edl_last_update_job_nam,
TD.brand_cd,
row_number() over (
partition by td.transaction_key,td.transaction_line_num
order by 
                              CASE bc.brand_customer_transaction_role_cd 
                                             WHEN 'BUYER' THEN 1 
                                             WHEN 'PAYER' THEN 2 
                                             ELSE 1 
                              END ) as rn,
bc.brand_customer_transaction_role_cd
FROM  edl_conform.transaction_detail TD
LEFT JOIN edl_conform.transaction TM
ON TD.transaction_key = TM.transaction_key
LEFT JOIN  edl_conform.brand_customer_transaction_xref BC
ON TD.transaction_key = BC.transaction_key
LEFT JOIN edl_conform.store ST
ON TD.original_transaction_store_id = ST.store_id
left join (select * 
from edl_stage.plus_sales_transaction_detail_hist  
where transaction_dt between '2019-03-29' and '2020-04-04' ) tdh
on 
TD.transaction_key=TO_HEX(MD5( CONCAT(case when tdh.selling_chain_nbr = 4 
								THEN 'CA|'
								when tdh.selling_chain_nbr = 7 
								THEN 'LB|' 
							end
							,cast(tdh.selling_store_nbr as string)
							,"|",cast(tdh.register_nbr as string)
							,"|",cast(tdh.transaction_dt as string)
							,"|",cast(tdh.transaction_nbr as string) 
				)))
and TD.transaction_line_num	= tdh.detail_seq_nbr		
) as a where rn=1
and transaction_dt	between '2019-03-29' and '2020-04-04' 