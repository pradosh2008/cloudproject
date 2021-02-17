run_bq_job -e plus_store_sku_inventory.bq &
#Rajesh - Added - plus_sku_dc_inventory
run_bq_job -e plus_sku_dc_inventory.bq &
wait
