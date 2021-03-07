-- Run this two command as we are dynamically loading partitions.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- check the count before insertion
select batch_id from ascena_staging_dev.   jus_hst_customers_20190104             group by batch_id;---20190130

select count(*) from ascena_staging_dev.jus_hst_customers_dr; --60710680
select count(*) from ascena_staging_dev.    jus_hst_customers_20190104            ; -- 60269525

desc ascena_staging_dev.jus_hst_customers_dr;
desc ascena_staging_dev.   jus_hst_customers_20190104            ;


INSERT INTO TABLE ascena_staging_dev.jus_hst_customers_dr PARTITION(batch_id=20190101) 
select 
address_1
,address_2
,address_rec_type
,bill_to_ship_to_code
,birth_year_month
,ca_last_update_date
,ca_last_update_job_id
,ca_last_update_source_code
,carrier_route_code
,child_flag
,city
,closest_store_distance
,closest_store_no
,coppa_flag
,corporate_individual_id
,country
,create_date
,create_job_id
,create_source_code
,customer_type
,deceased_flag
,delivery_point_barcode
,direct_mail_preference_code
,dma_do_not_mail_flag
,do_not_contact_flag
,do_not_list_share_flag
,do_not_sell_name_flag
,do_not_statement_insert_flag
,dpv_vacancy_flag
,dsf_business_address_flag
,dsf_seasonal_address_flag
,email_preference_code
,fips_code
,first_name
,foreign_address_flag
,gender_code
,geo_match_level
,household_id
,individual_id
,last_name
,last_ncoa_date
,last_update_job_id
,last_update_source_code
,last_update_time
,latitude
,longitude
,lot_code
,loyalty_no
,loyalty_store_no
,mailability_score
,mass_individual_flag
,middle_name
,military_address_flag
,move_effective_date
,name_address_updt_source_code
,name_prefix
,name_suffix
,ncoa_return_addr_source_code
,ncoalink_match_code
,net_sales_lifetime_limited_too
,new_to_file_reactivated_code
,next_closest_store_distance
,next_closest_store_no
,nursing_home_address_flag
,plcc_card_type
,plcc_last_purchase_date
,plcc_status_code
,po_box_flag
,prison_address_flag
,profanity_flag
,residential_id
,return_mail_flag
,return_mail_flagged_date
,state_province
,text_message_preference_code
,trans_loyalty_store_12_mo
,zip_4
,zip_postal_code
FROM ascena_staging_dev. jus_hst_customers_20190104     where batch_id=20190130;

--check the count . It should be sum of both the count
select count(*) from ascena_staging_dev.jus_hst_customers_dr; --120980205


CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_customers_drc
AS 
WITH MID AS
(SELECT 
TBL.individual_id
,TBL.MAX_BATCH_ID     
from
(
SELECT
individual_id as individual_id
,BATCH_ID AS MAX_BATCH_ID
,row_number() over(
partition by individual_id order by BATCH_ID desc) as rn
from    ascena_staging_dev.jus_hst_customers_dr 
) as TBL
where   rn=1)
SELECT TBL1.* 
FROM ASCENA_STAGING_DEV.jus_hst_customers_dr  TBL1
JOIN MID
ON TBL1.BATCH_ID = MID.MAX_BATCH_ID
AND TBL1.individual_id = MID.individual_id;
                

select count(*) from ascena_staging_dev.jus_hst_customers_drc;
