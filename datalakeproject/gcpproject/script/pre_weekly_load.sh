#!/usr/bin/bash

. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh

#echo "pre_sap_store_inventory.bq" >/data/ctl/pre_sap_store_inventory.ctl
#echo "pre_sap_plan_data.bq" >/data/ctl/pre_sap_plan_data.ctl
echo "pre_acc_df_prod_hier.bq" >/${env_data_root}/ctl/pre_acc_df_prod_hier.ctl

