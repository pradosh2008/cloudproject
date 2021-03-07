-- Run this two command as we are dynamically loading partitions.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- check the count before insertion
select count(*) from ascena_staging_dev.jus_hst_transaction_discounts_dr; --194923805
select batch_id from ascena_staging_dev.jus_hst_transaction_discounts_dr group by batch_id;--20190301
select batch_id from ascena_staging_dev.jus_hst_transaction_discounts_20190104 group by batch_id; --20190205
select count(*) from ascena_staging_dev.jus_hst_transaction_discounts_20190104 where batch_id=20190205; -- 96550265


INSERT INTO TABLE ascena_staging_dev.jus_hst_transaction_discounts_dr PARTITION(batch_id=20190101) 
select 
transaction_id
,line_no
,offer_type_code
,offer_code
,transaction_date
,store_id
,individual_id
,offer_code_id
,discount_amount
,coupon_count
,create_date
,create_job_id
,last_update_date
,last_update_job_id
,lto_individual_id
,sale_return_code
,coupon_no
FROM ascena_staging_dev.jus_hst_transaction_discounts_20190104 where batch_id=20190205;


--check the count . It should be sum of both the count
select count(*) from ascena_staging_dev.jus_hst_transaction_discounts_dr; --291474070