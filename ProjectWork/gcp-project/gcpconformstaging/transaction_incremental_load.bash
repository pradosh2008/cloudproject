###################Stats post First Load################
jdbc:hive2://edllbphmg01.prod.ascena.com:2> select count(*) from ascena_analytic_mart.transaction;
 197850988

SELECT count(*) FROM `p-asna-analytics-002.edl_landing.transaction`
197850988

SELECT count(*) FROM `p-asna-analytics-002.edl_conform.transaction`
197850988

#############stats before running incremental load############
jdbc:hive2://edllbphmg01.prod.ascena.com:2> select count(*) from ascena_conform.transaction;

198480273


############Steps followed to do incremental load##############

#load 10 days of data into transaction table
run_hql -f transaction.hql 10


#clean up the landing bucket
gsutil -m rm gs://p-asna-datasink-003/transaction/*

#copy it to bucket (overwritten to the existing bucket)
gscp -d ascena_analytic_mart transaction>transaction.out 2>&1

#run the bq query table to recreate the landing table
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.transaction  'gs://p-asna-datasink-003/transaction/*'

################Add new records from landing table to conform table############## 
bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --append_table --destination_table edl_conform.transaction --use_legacy_sql=false 'select lt.*
from edl_landing.transaction lt
left join edl_conform.transaction ct
    on lt.transaction_key = ct.transaction_key
where ct.transaction_key is null'


############stats post incremental load#########
SELECT count(*) FROM `p-asna-analytics-002.edl_conform.transaction`
198480273 

#############Post incremental load#############
bq cp --force edl_landing.transaction edl_archive.transaction


