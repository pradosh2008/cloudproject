#!/usr/bin/bash

. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh


#run the PLUS mart load 
run_bq_job -e "dim_item.bq"  &
run_bq_job -e "dim_store.bq" &
#run_bq_job -e "fact_transaction.bq" &
#run_bq_job -e "fact_transaction_detail.bq" &
wait
run_bq_job -e "fact_transaction_merch_line.bq" 
