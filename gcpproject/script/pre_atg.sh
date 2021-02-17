#!/usr/bin/bash

. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh


echo "ann_atg_returns.bq"      >/${env_data_root}/ctl/atg_returns_${$}-`date +%m%d%H%M%S`.ctl
echo "ann_atg_transactions.bq" >/${env_data_root}/ctl/atg_transactions_${$}-`date +%m%d%H%M%S`.ctl
