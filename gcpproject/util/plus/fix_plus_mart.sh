fix_dim_item.bq >fix_dim_item.out 2>&1 &
fix_dim_store.bq >fix_dim_store.out 2>&1 &
fix_fact_transaction_fee.bq >fix_fact_transaction_fee.out 2>&1 &
fix_fact_transaction_notes.bq >fix_fact_transaction_notes.out 2>&1 &
fix_fact_transaction_tax.bq >fix_fact_transaction_tax.out 2>&1 &
fix_fact_transaction_tender.bq >fix_fact_transaction_tender.out 2>&1 &

wait
tail fix_dim_item.out 
tail fix_dim_store.out 
tail fix_fact_transaction_fee.out 
tail fix_fact_transaction_notes.out
tail fix_fact_transaction_tax.out
tail fix_fact_transaction_tender.out