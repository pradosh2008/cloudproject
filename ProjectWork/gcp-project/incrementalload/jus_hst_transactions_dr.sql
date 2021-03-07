-- Run this two command as we are dynamically loading partitions.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- check the count before insertion
select count(*) from ascena_staging_dev.jus_hst_transactions_dr; --28,108,791
select batch_id from ascena_staging_dev.jus_hst_transactions_20190104 group by batch_id; --20190201
select count(*) from ascena_staging_dev.jus_hst_transactions_20190104 where batch_id=20190201; -- 32,431,807


INSERT INTO TABLE ascena_staging_dev.jus_hst_transactions_dr PARTITION(batch_id=20190101) 
select 
transaction_id
,transaction_date
,store_no
,register_no
,transaction_number
,ship_date
,store_id
,individual_id
,marketing_offer_code_id
,matching_results_id
,matching_results_date
,transaction_type_code
,purchase_gross_amount
,purchase_mktg_discount_amount
,purchase_mrch_discount_amount
,purchase_net_amount
,purchase_item_count
,departments_shopped_count
,return_gross_amount
,return_mktg_discount_amount
,return_mrch_discount_amount
,return_net_amount
,return_item_count
,original_phone_no
,phone_no
,phone_scrub_reason_code
,customer_no
,ecommerce_customer_no
,loyalty_no
,employee_flag
,associate_no
,create_date
,create_job_id
,last_update_date
,last_update_job_id
,lto_individual_id
,purchase_other_discount_amount
,return_other_discount_amount
,email_address_id
,total_deflated_cost
,total_actual_cost
,e_receipt_flag
,birthday_month_1
,birthday_year_1
,birthday_month_2
,birthday_year_2
,birthday_month_3
,birthday_year_3
,birthday_month_4
,birthday_year_4
,birthday_month_5
,birthday_year_5
,birthday_name_1
,birthday_name_2
,birthday_name_3
,birthday_name_4
,birthday_name_5
,ois
,transaction_datetime                            
FROM ascena_staging_dev.jus_hst_transactions_20190104 where batch_id=20190201;


--check the count . It should be sum of both the count
select count(*) from ascena_staging_dev.jus_hst_transactions_dr; --60540598


