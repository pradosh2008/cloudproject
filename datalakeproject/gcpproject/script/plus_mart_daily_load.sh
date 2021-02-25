run_bq_job -e dim_item.bq &
run_bq_job -e dim_store.bq &
run_bq_job -e dim_brand_customer.bq &
wait
run_bq_job -e fact_transaction.bq &
run_bq_job -e fact_transaction_detail.bq &
run_bq_job -e fact_transaction_discount.bq &
run_bq_job -e fact_transaction_notes.bq &
run_bq_job -e fact_transaction_tender.bq &
run_bq_job -e fact_transaction_tax.bq &
run_bq_job -e fact_transaction_merch_line.bq &
run_bq_job -e fact_transaction_fee.bq &
wait
#rittika - added - transaction_customer_xref
run_bq_job -e transaction_customer_xref.bq &
#Sumant SK - Added - CRM Tables 20200507
run_bq_job -e plus_brand_customer.bq &
run_bq_job -e plus_brand_customer_demographics.bq &
run_bq_job -e plus_brand_customer_phone.bq &
run_bq_job -e plus_brand_customer_email.bq &
run_bq_job -e plus_brand_customer_email_outbound.bq &
run_bq_job -e plus_brand_customer_perks_membership.bq &
run_bq_job -e plus_brand_customer_opt_in_out.bq &
run_bq_job -e plus_brand_customer_account.bq &
wait
#Karthik - Added - CRM Marketing Mart Tables to Daily load
run_bq_job -e plus_brand_customer_mailing.bq &
run_bq_job -e plus_brand_customer_direct_marketing_cell.bq &
run_bq_job -e plus_brand_customer_clcst_gp.bq &
wait
#Rajesh - Added - Merch Plan Forecast Mart Tables to Daily load
run_bq_job -e plus_fact_demand_sales_day.bq &
run_bq_job -e plus_fact_sales_day.bq &
run_bq_job -e plus_fact_subclass_store_day.bq &
run_bq_job -e plus_fact_merch_plan_week.bq &
run_bq_job -e plus_fact_demand_sales_week.bq &
wait
#Rajesh - Added - Plus Fact Inventory DC Week Mart Table to Daily load
run_bq_job -e plus_fact_inventory_dc_week.bq &
run_bq_job -e plus_fact_inventory_store_week.bq &
wait
#Rajesh - Added - Dim Subclass Mart Table to Daily load
run_bq_job -e dim_subclass.bq &
wait
#Rajesh - Added - Plus Fact On Order Mart Table to Daily load
run_bq_job -e plus_fact_on_order_sku_dc_day.bq &
wait

