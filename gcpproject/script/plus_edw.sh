run_bq_job -e plus_sales_transaction_header.bq &
run_bq_job -e plus_sales_transaction_detail.bq &
run_bq_job -e plus_sales_transaction_discount.bq &
run_bq_job -e plus_sales_transaction_fee.bq &
run_bq_job -e plus_sales_transaction_notes.bq &
run_bq_job -e plus_sales_transaction_payment.bq &
run_bq_job -e plus_sales_transaction_tax.bq &
run_bq_job -e plus_sales_transaction_tender.bq &
run_bq_job -e plus_sales_customer.bq &
run_bq_job -e plus_demand_sales_sku_store_day.bq &
run_bq_job -e plus_sales_sku_store_day.bq &
run_bq_job -e plus_subclass_store_day.bq &
run_bq_job -e plus_merch_plan_week.bq &
#Rajesh - Added - plus_on_order_sku_dc_day stage table to Daily load
run_bq_job -e plus_on_order_sku_dc_day.bq &
wait

