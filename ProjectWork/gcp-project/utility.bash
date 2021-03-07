gsutil ls -l gs://p-asna-datasink-003/brand_customer/
gsutil ls -l gs://p-asna-datasink-003/brand_customer_location/
gsutil ls -l gs://p-asna-datasink-003/brand_customer_email/
gsutil ls -l gs://p-asna-datasink-003/brand_customer_phone/
gsutil ls -l gs://p-asna-datasink-003/brand_customer_membership/
gsutil ls -l gs://p-asna-datasink-003/brand_customer_account/
gsutil ls -l gs://p-asna-datasink-003/brand_customer_addr/
gsutil ls -l gs://p-asna-datasink-003/brand_customer_transaction_xref/
gsutil ls -l gs://p-asna-datasink-003/store/
gsutil ls -l gs://p-asna-datasink-003/item/
gsutil ls -l gs://p-asna-datasink-003/transaction/
gsutil ls -l gs://p-asna-datasink-003/transaction_detail/
gsutil ls -l gs://p-asna-datasink-003/transaction_tender/
gsutil ls -l gs://p-asna-datasink-003/transaction_discount/
gsutil ls -l gs://p-asna-datasink-003/transaction_fee/
gsutil ls -l gs://p-asna-datasink-003/transaction_notes/


#COMPLETED LOADS#
gscp -d ascena_analytic_mart brand_customer_location>brand_customer_location.out 2>&1
gscp -d ascena_analytic_mart brand_customer_transaction_xref>brand_customer_transaction_xref.out 2>&1

#CUSTOMER TABLE LOADS#
gscp -d ascena_analytic_mart brand_customer>brand_customer.out 2>&1
gscp -d ascena_analytic_mart brand_customer_email>brand_customer_email.out 2>&1
gscp -d ascena_analytic_mart brand_customer_phone>brand_customer_phone.out 2>&1
gscp -d ascena_analytic_mart brand_customer_membership>brand_customer_membership.out 2>&1
gscp -d ascena_analytic_mart brand_customer_account>brand_customer_account.out 2>&1
gscp -d ascena_analytic_mart brand_customer_addr>brand_customer_addr.out 2>&1


#MISC TABLE LOADS#
gscp -d ascena_analytic_mart store>store.out 2>&1
gscp -d ascena_analytic_mart item>item.out 2>&1


#TRANSACTION TABLE LOADS#
gscp -d ascena_analytic_mart transaction>transaction.out 2>&1
gscp -d ascena_analytic_mart transaction_detail>transaction_detail.out 2>&1
gscp -d ascena_analytic_mart transaction_tender>transaction_tender.out 2>&1
gscp -d ascena_analytic_mart transaction_discount>transaction_discount.out 2>&1
gscp -d ascena_analytic_mart transaction_fee>transaction_fee.out 2>&1
gscp -d ascena_analytic_mart transaction_notes>transaction_notes.out 2>&1
gscp -d ascena_analytic_mart transaction_tax>transaction_tax.out 2>&1



bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.brand_customer  'gs://p-asna-datasink-003/brand_customer/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.brand_customer_location  'gs://p-asna-datasink-003/brand_customer_location/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.brand_customer_email  'gs://p-asna-datasink-003/brand_customer_email/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.brand_customer_phone  'gs://p-asna-datasink-003/brand_customer_phone/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.brand_customer_membership  'gs://p-asna-datasink-003/brand_customer_membership/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.brand_customer_account  'gs://p-asna-datasink-003/brand_customer_account/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.brand_customer_addr  'gs://p-asna-datasink-003/brand_customer_addr/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.brand_customer_transaction_xref  'gs://p-asna-datasink-003/brand_customer_transaction_xref/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.store  'gs://p-asna-datasink-003/store/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.item  'gs://p-asna-datasink-003/item/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.transaction  'gs://p-asna-datasink-003/transaction/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.transaction_detail  'gs://p-asna-datasink-003/transaction_detail/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.transaction_tender  'gs://p-asna-datasink-003/transaction_tender/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.transaction_discount  'gs://p-asna-datasink-003/transaction_discount/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.transaction_fee  'gs://p-asna-datasink-003/transaction_fee/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.transaction_notes  'gs://p-asna-datasink-003/transaction_notes/*'
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.transaction_tax  'gs://p-asna-datasink-003/transaction_tax/*'





hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/brand_customer/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/brand_customer_email/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/brand_customer_location/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/brand_customer_transaction_xref/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/brand_customer_phone/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/brand_customer_membership/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/brand_customer_account/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/brand_customer_addr/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/store/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/item/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/transaction/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/transaction_detail/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/transaction_tender/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/transaction_discount/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/transaction_fee/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/transaction_notes/
hdfs dfs -ls hdfs://ascenaprod/apps/hive/warehouse/ascena_analytic_mart.db/transaction_tax/



select count(*) from ascena_analytic_mart.brand_customer;
select count(*) from ascena_analytic_mart.brand_customer_location;
select count(*) from ascena_analytic_mart.brand_customer_email;
select count(*) from ascena_analytic_mart.brand_customer_phone;
select count(*) from ascena_analytic_mart.brand_customer_membership;
select count(*) from ascena_analytic_mart.brand_customer_account;
select count(*) from ascena_analytic_mart.brand_customer_addr;
select count(*) from ascena_analytic_mart.brand_customer_transaction_xref;
select count(*) from ascena_analytic_mart.store;
select count(*) from ascena_analytic_mart.item;
select count(*) from ascena_analytic_mart.transaction;
select count(*) from ascena_analytic_mart.transaction_detail;
select count(*) from ascena_analytic_mart.transaction_tender;
select count(*) from ascena_analytic_mart.transaction_discount;
select count(*) from ascena_analytic_mart.transaction_fee;
select count(*) from ascena_analytic_mart.transaction_notes;
select count(*) from ascena_analytic_mart.transaction_tax;


SELECT count(*) FROM `p-asna-analytics-002.edl_landing.brand_customer`; 
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.brand_customer_location`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.brand_customer_email`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.brand_customer_phone`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.brand_customer_membership`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.brand_customer_account`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.brand_customer_addr`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.brand_customer_transaction_xref`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.store`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.item`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.transaction`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.transaction_detail`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.transaction_tender`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.transaction_discount`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.transaction_fee`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.transaction_notes`;
SELECT count(*) FROM `p-asna-analytics-002.edl_landing.transaction_tax`;


 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.brand_customer --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.brand_customer`' 
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.brand_customer_location --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.brand_customer_location`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.brand_customer_email --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.brand_customer_email`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.brand_customer_phone --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.brand_customer_phone`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.brand_customer_membership --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.brand_customer_membership`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.brand_customer_account --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.brand_customer_account`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.brand_customer_addr --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.brand_customer_addr`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.brand_customer_transaction_xref --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.brand_customer_transaction_xref`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.store --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.store`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.item --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.item`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.transaction --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.transaction`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.transaction_detail --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.transaction_detail`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.transaction_tender --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.transaction_tender`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.transaction_discount --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.transaction_discount`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.transaction_fee --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.transaction_fee`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.transaction_notes --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.transaction_notes`'
 
 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.transaction_tax --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.transaction_tax`'


gsutil -m rm -r gs://p-asna-datasink-003/brand_customer/
gsutil -m rm -r gs://p-asna-datasink-003/brand_customer_location/
gsutil -m rm -r gs://p-asna-datasink-003/brand_customer_email/
gsutil -m rm -r gs://p-asna-datasink-003/brand_customer_phone/
gsutil -m rm -r gs://p-asna-datasink-003/brand_customer_membership/
gsutil -m rm -r gs://p-asna-datasink-003/brand_customer_account/
gsutil -m rm -r gs://p-asna-datasink-003/brand_customer_addr/
gsutil -m rm -r gs://p-asna-datasink-003/brand_customer_transaction_xref/
gsutil -m rm -r gs://p-asna-datasink-003/store/
gsutil -m rm -r gs://p-asna-datasink-003/item/
gsutil -m rm -r gs://p-asna-datasink-003/transaction/
gsutil -m rm -r gs://p-asna-datasink-003/transaction_detail/
gsutil -m rm -r gs://p-asna-datasink-003/transaction_tender/
gsutil -m rm -r gs://p-asna-datasink-003/transaction_discount/
gsutil -m rm -r gs://p-asna-datasink-003/transaction_fee/
gsutil -m rm -r gs://p-asna-datasink-003/transaction_notes/	
	
	

gsutil -m rm -r gs://p-asna-datasink-003/brand_customer/
gsutil -m rm -r gs://p-asna-datasink-003/brand_customer_phone/
gsutil -m rm -r gs://p-asna-datasink-003/store/	
gsutil -m rm -r gs://p-asna-datasink-003/transaction/
gsutil -m rm -r gs://p-asna-datasink-003/transaction_fee/
gsutil -m rm -r gs://p-asna-datasink-003/transaction_notes/	

--dropping a table


 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table work.brand_customer_location --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.brand_customer_location`'
 
bq query --use_legacy_sql=false 'drop table `p-asna-analytics-002.work.brand_customer_location`'


bq cp --force edl_conform.brand_customer edl_archive.brand_customer
bq cp --force edl_conform.brand_customer_phone edl_archive.brand_customer_phone
bq cp --force edl_conform.store edl_archive.store

bq cp --force edl_conform.transaction edl_archive.transaction
bq cp --force edl_conform.transaction_fee analytic_mart.transaction_fee
bq cp --force edl_conform.transaction_notes edl_archive.transaction_notes


bq cp --force edl_stage.pre_ann_calendar edl_archive.pre_ann_calendar



----------------------------------------

select max(EXTRACT(DATE FROM (PARSE_DATETIME('%m/%d/%Y %H:%M:%S',entry_date_time)))),max(transaction_date),max(batch_id) from ascena_staging.ann_ann_aw_transactions_header;

select max(date_format(from_unixtime(unix_timestamp(entry_date_time,'mm/dd/yyyy hh:mm:ss')),'yyyy-MM-dd')),max(transaction_date),max(batch_id) from ascena_staging.ann_ann_aw_transactions_header;


--select count(*) from edl_landing.ann_ann_aw_transactions_header ; --32136258

--select max(entry_date_time),max(transaction_date),max(batch_id) from edl_landing.ann_ann_aw_transactions_header ; 
--12/31/2018 23:59:40   2019-06-12  20190612

--select count(*) from work.ann_ann_aw_transactions_header ; --32136258
--select max(entry_date_time),max(transaction_date),max(batch_id)  from work.ann_ann_aw_transactions_header ; 



select count(*) from (select 
transaction_key
,count(*)
from 
ascena_analytic_mart.transaction
group by
transaction_key
having count(*)>1)a;


select 
transaction_key
,transaction_fee_line_num
,count(*)
from 
ascena_analytic_mart.transaction_fee
group by
transaction_key
,transaction_fee_line_num
having count(*)>1 limit 5;


select 
transaction_key
,transaction_fee_line_num
,count(*)
from 
ascena_analytic_mart.transaction_fee
group by
transaction_key
,transaction_fee_line_num
having count(*)>1 limit 5;

select 
transaction_key
,transaction_fee_line_num
from 
ascena_analytic_mart.transaction_fee
where transaction_fee_line_num is null limit 5;
	
select count(*) from (select 
transaction_key
,transaction_notes_line_num
,transaction_notes_sequence_num
,count(*)
from 
ascena_analytic_mart.transaction_notes
group by
transaction_key
,transaction_notes_line_num
,transaction_notes_sequence_num
having count(*)>1)a;

--6

select 
transaction_key
,transaction_notes_line_num
,transaction_notes_sequence_num
from 
ascena_analytic_mart.transaction_notes
where transaction_notes_sequence_num is null or transaction_notes_line_num is null limit 5;


select count(*) from (select 
transaction_key
,transaction_notes_line_num
,count(*)
from 
ascena_analytic_mart.transaction_notes
group by
transaction_key
,transaction_notes_line_num
having count(*)>1)a;

select count(*) from (select 
transaction_key
,count(*)
from 
ascena_conform.transaction
group by
transaction_key
having count(*)>1)a;



0: jdbc:hive2://edllbphmg01.prod.ascena.com:2> select count(*) from ascena_conform.transaction;
+------------+--+
|    _c0     |
+------------+--+
| 198765483  |
+------------+--+
1 row selected (0.251 seconds)
0: jdbc:hive2://edllbphmg01.prod.ascena.com:2> select count(*) from ascena_conform.transaction_fee;
+-----------+--+
|    _c0    |
+-----------+--+
| 57872594  |
+-----------+--+
1 row selected (0.131 seconds)
0: jdbc:hive2://edllbphmg01.prod.ascena.com:2> select count(*) from ascena_conform.transaction_notes;
+------------+--+
|    _c0     |
+------------+--+
| 126953888  |
+------------+--+
1 row selected (0.132 seconds)
0: jdbc:hive2://edllbphmg01.prod.ascena.com:2>


	
run_hql -f brand_customer.hql 14	
run_hql -f brand_customer_phone.hql 14
run_hql -f store.hql 14
run_hql -f transaction.hql 1000
run_hql -f transaction_fee.hql 1000
run_hql -f transaction_notes.hql 1000


	
brand_customer_account.hql   brand_customer_membership.hql        transaction_discount.hql
brand_customer_account.out   brand_customer_phone.hql             transaction_discount.out
brand_customer_addr.hql      brand_customer_transaction_xref.hql  transaction_fee.hql
brand_customer_addr.out      item_analytic.out                    transaction.hql
brand_customer_email.hql     item.hql                             transaction_notes.hql
brand_customer_email.out     item.out                             transaction_tax.hql
brand_customer.hql           store.hql                            transaction_tender.hql
brand_customer_location.hql  test.hql                             transaction_tender.out
brand_customer_location.out  transaction_detail.hql


 
brand_customer 
brand_customer_location 
brand_customer_email 
brand_customer_phone
brand_customer_membership
brand_customer_account
brand_customer_addr
brand_customer_transaction_xref
store
item
transaction
transaction_detail
transaction_tender
transaction_discount
transaction_fee
transaction_notes
transaction_tax





bq cp --force edl_conform.brand_customer work.temp_customer

--SELECT count(*) FROM `p-asna-analytics-002.edl_conform.brand_customer` LIMIT 1000
--56031677
--INSERT `p-asna-analytics-002.work.temp_customer` (brand_customer_key) VALUES('0')
--SELECT count(*) FROM `p-asna-analytics-002.work.temp_customer` LIMIT 1000

select count(*) from (select 
transaction_key
,count(*)
from 
`p-asna-analytics-002.edl_conform.transaction`
group by
transaction_key
having count(*)>1)a;

-------- to do 

bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.transaction  'gs://p-asna-datasink-003/transaction/*'

bq rm --force edl_conform.transaction
bq rm --force analytic_mart.fact_transaction

 bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_conform.transaction --use_legacy_sql=false 'SELECT * FROM `p-asna-analytics-002.edl_landing.transaction`'
 
 
---------------
SELECT case when CHAR_LENGTH(transaction_tm)=1 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(transaction_dt as string),' ','000',transaction_tm))
			when CHAR_LENGTH(transaction_tm)=2 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(transaction_dt as string),' ','00',transaction_tm))
			when CHAR_LENGTH(transaction_tm)=3 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(transaction_dt as string),' ','0',transaction_tm))
			when CHAR_LENGTH(transaction_tm)=4 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(transaction_dt as string),' ',transaction_tm))
       else PARSE_DATETIME('%Y-%m-%d %H%M%S', concat(cast(transaction_dt as string),' ',transaction_tm))
       end as transaction_tms from `edl_conform.transaction`  limit 100;



select order_receive_dt,order_receive_tm,case when CHAR_LENGTH(order_receive_tm)=1 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(order_receive_dt as string),' ','000',order_receive_tm))
			when CHAR_LENGTH(order_receive_tm)=2 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(order_receive_dt as string),' ','00',order_receive_tm))
			when CHAR_LENGTH(order_receive_tm)=3 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(order_receive_dt as string),' ','0',order_receive_tm))
			when CHAR_LENGTH(order_receive_tm)=4 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(order_receive_dt as string),' ',order_receive_tm))
       else PARSE_DATETIME('%Y-%m-%d %H%M%S', concat(cast(order_receive_dt as string),' ',order_receive_tm))
       end as order_tms from `edl_conform.transaction` limit 10;	   
 
analytic_mart
-------------------------------------
integrate the payer and buyer logic
instead of time column , club date and time and make it a datetime field while building the conform layer table 
placing order_dt next to transaction-dt
Though mart should have only date column , does not need to have tms column


 select count(*) from `edl_conform.transaction` where transaction_key in(
 'b1fc930fe053e2cfe71644d68b98a702'  
 ,'520ce5fff6c5c0d06433f884be3235cc'  
 ,'67bcdc76806280ee4fba10638fdad94a'  
 ,'1914b26f5a4c025e1f850f97108a82d3'  
 ,'bc4b41746668cd6054394433f75acf1c'  
 ,'0462671b87d73d8cb24cdbb4c47a7ee8'  
 ,'dc0c8bc01efdeea1c771bf7cc81e9470'  
 ,'4e4f936bd5457453b966b4786db7320e'  
 ,'38a5543bebbd25c93da671198833aeb4'  
 ,'69824108916b6b40990845ba7a8b4896'  
 ,'7c383d948d305260627b0cce99a70a4d'  
 ,'48ad9729d85b4a413749ca89764c78c7'  
 ,'3d8977286044a8a263acfe0517f84864'  
 ,'596cad7910245cfcea4a89fe89d2cfb9'  
 ,'42ced13e02f2a080c0fbe708fd7285e8'  
 ,'7b158a34daa1beef8ac3c37143f1c49f'  
 ,'565b595306ed8c4b2b539c66af8c33d3' );

 
ssh gcpinteg@10.16.224.43

-------------------------------------------------------------------------------------------------------

SELECT max( c.fiscal_week_id),min( c.fiscal_week_id) FROM `p-asna-analytics-002.analytic_mart.pre_store_inventory`  as a
left outer join
(select fiscal_week_id
        ,day_dt
from edl_stage.pre_ann_calendar
    where fiscal_day_of_week=1) as c
on a.fiscal_week_dt =c.day_dt  
