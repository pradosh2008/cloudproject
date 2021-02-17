fix_pre_item.bq >fix_pre_item.out 2>&1 &
fix_pre_transaction_header.bq >fix_pre_transaction_header.out 2>&1 &
fix_pre_transaction_item.bq >fix_pre_transaction_item.out 2>&1 &
wait
tail fix_pre_item.out 
tail fix_pre_transaction_header.out 
tail fix_pre_transaction_item.out 
