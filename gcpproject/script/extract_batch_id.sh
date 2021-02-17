#!/usr/bin/bash

#to extract batch_id
# parse files with file_name_prefix_YYYYMMMDD* pattern

files_in_gcp=($(gsutil ls gs://p-asna-datasink-003/pre/ANN_ACC_DF_PROD_HIER_201911*/))

printf "%s\n" "${files_in_gcp[@]}"

cnt=${#files_in_gcp[@]}

for ((i=0;i<cnt;i++)); do
#creating the landing table
#bq load --autodetect --replace --source_format=CSV --quote="" edl_landing.ann_acc_df_prod_hier1 "${files_in_gcp[i]}"
#rc_check $? "Load edl_landing"

a=$ expr substr ${files_in_gcp[i]} 51 8

echo hi
bq query --project_id='p-asna-analytics-002' --use_legacy_sql=false <<!
SELECT $a
!


done

#for ((i=0;i<cnt;i++));do
#    a=$ expr substr ${files_in_gcp[i]} 51 8
#echo $a
#done

# less code to do the top bit
gsutil ls 'gs://p-asna-datasink-003/pre/ANN_ACC_DF_PROD_HIER_*' | (
while read files_in_gcp do
    batch_id=$ expr substr ${files_in_gcp} 51 8
    ann_prod_hier.bq ${files_in_gcp} $batch_id
done
)
