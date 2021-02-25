fix_pre_atg_order_commerceitemadjustments.bq >fix_pre_atg_order_commerceitemadjustments.out 2>&1 &
fix_pre_atg_order_commerceItem.bq >fix_pre_atg_order_commerceItem.out 2>&1 &
fix_pre_atg_order_commerceitemcoupons.bq >fix_pre_atg_order_commerceitemcoupons.out 2>&1 &
fix_pre_atg_order_header.bq >fix_pre_atg_order_header.out 2>&1 &
fix_pre_atg_order_payment.bq >fix_pre_atg_order_payment.out 2>&1 &
fix_pre_atg_order_return.bq >fix_pre_atg_order_return.out 2>&1 &
fix_pre_atg_order_shipment.bq >fix_pre_atg_order_shipment.out 2>&1 &
fix_pre_atg_order_shipment_tracking.bq >fix_pre_atg_order_shipment_tracking.out 2>&1 &
fix_pre_aw_transaction_header.bq >fix_pre_aw_transaction_header.out 2>&1 &
wait
