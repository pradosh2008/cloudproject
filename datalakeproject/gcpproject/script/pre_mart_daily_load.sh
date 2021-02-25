#!/usr/bin/bash

. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh

export L_SUPPORT_EMAILS="GIC_AnalyticsCoE_Architecture@AscenaRetail.com,Christian.Snediker-Morscheck@ascenaretail.com"

#Staging Status CHECK
options=" --quiet --format=csv --use_legacy_sql=false"


sql='SELECT count(*) 
FROM edl_stage.bq_job_defn d
left join edl_stage.bq_job_status s
    on d.bq_name = s.bqScriptName
    and rundate = current_date
    and runStatus = "Success"
where job_type = "Stage" 
and brand_cd = "PRE"
and s.bqScriptName is null
'
sql='select count(*) from  edl_stage.bq_job_status where 1=0'

run_validation "$sql" "Check Stage jObs"

if [ -z "$ERROR_MSG" ]; then
   echo "failure"    
   echo "Stage load failed with " $ERROR_CD " bq scripts"
   echo $'There is a failure in Staging load\nPlease check edl_stage.bq_job_status for details.\n'$ERROR_MSG|mail -s "Do Not Reply - Failure in $run_env GCP Stage Load" ${L_SUPPORT_EMAILS}    
else
    echo "success"  
    pid=$$
fi
    echo "pre_transaction_header.bq" >/${env_data_root}/ctl/pre_transaction_header-${pid}.ctl
    echo "pre_transaction_item.bq" >/${env_data_root}/ctl/pre_transaction_item-${pid}.ctl
    echo "pre_item.bq" >/${env_data_root}/ctl/pre_item-${pid}.ctl
    echo "pre_store.bq" >/${env_data_root}/ctl/pre_store-${pid}.ctl
    echo "pre_style.bq" >/${env_data_root}/ctl/pre_style-${pid}.ctl
    echo "pre_class.bq" >/${env_data_root}/ctl/pre_class-${pid}.ctl



