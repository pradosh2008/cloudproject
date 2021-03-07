create or replace table analytic_mart_qa.fact_transaction_detail as
select 
 d.transaction_key
,d.transaction_line_num
,d.transaction_dt
,d.order_dt
,d.store_key
,d.brand_customer_key
,d.item_key
,d.return_ind
,d.original_transaction_header_key
,d.original_transaction_store_key
,d.original_transaction_dt
,d.original_transaction_line_num
,d.line_action_cd
,d.line_object_type_cd
,d.line_object_cd
,d.line_void_ind
,d.scanned_ind
,d.gift_card_ind
,d.taxable_ind
,d.style_locator_ind
,d.permanent_markdown_type_cd
,d.return_reason_cd
,d.return_disposition_cd
,d.return_reason_desc
,d.return_to_dc_ind
,d.layaway_ind
,d.order_ind
,case when d.transaction_dt between '2019-03-29' and '2020-04-03'then dc.sku_cost_amt else	d.sku_cost_amt end as sku_cost_amt
,d.sku_quantity_num
,d.sku_retail_amt
,d.markdown_amt
,d.customer_paid_tax_amt
,d.company_owed_tax_amt
,d.commision_amt
,d.current_retail_amt
,d.original_unit_retail_amt
,d.primary_salesperson_id
,d.secondary_salesperson_id
,d.original_primary_salesperson_id
,d.original_secondary_salesperson_id
,d.upc_store_key
,d.upc_num
,d.size_cd
,d.cross_brand_store_key
,d.overseas_item_ind
,d.edl_create_tms
,d.edl_create_job_nam
,d.edl_last_update_tms
,d.edl_last_update_job_nam
,d.brand_cd
from analytic_mart_qa.fact_transaction_detail_backup d
left outer join analytic_mart.fact_transaction_detail_costamt_from_29032019_04042020 dc
on d.transaction_key = dc.transaction_key
and d.transaction_line_num =dc.transaction_line_num