-- Run this two command as we are dynamically loading partitions.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- check the count before insertion
select count(*) from ascena_staging_dev.jus_hst_transaction_details_dr; --205253703
select batch_id from ascena_staging_dev.jus_hst_transaction_details_dr group by batch_id;--20190301
select batch_id from ascena_staging_dev.jus_hst_transaction_details_20190105 group by batch_id; --20190205
select count(*) from ascena_staging_dev.jus_hst_transaction_details_20190105 where batch_id=20190205; -- 84089353


INSERT INTO TABLE ascena_staging_dev.jus_hst_transaction_details_dr PARTITION(batch_id=20190101) 
select 
transaction_id
,line_no
,transaction_date
,store_id
,individual_id
,ship_date
,merchandise_style_id
,department_no
,class_no
,style_no
,color_no
,size_no
,short_sku_no
,gross_price
,net_price
,original_price
,actual_cost
,deflated_cost
,markdown_flag
,sale_return_code
,create_date
,create_job_id
,last_update_date
,last_update_job_id
,lto_individual_id
,transaction_number
,item_code
,ois_flag
FROM ascena_staging_dev.jus_hst_transaction_details_20190105 where batch_id=20190205;


--check the count . It should be sum of both the count
select count(*) from ascena_staging_dev.jus_hst_transaction_details_dr; --289343056