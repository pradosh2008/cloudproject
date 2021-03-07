-- Run this two command as we are dynamically loading partitions.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- check the count before insertion
select count(*) from ascena_staging_dev.jus_hst_promotion_history_dr; --587404274
select batch_id from ascena_staging_dev.jus_hst_promotion_history_dr group by batch_id;--20190301
select batch_id from ascena_staging_dev.jus_hst_promotion_history_20190105 group by batch_id; --20190207
select count(*) from ascena_staging_dev.jus_hst_promotion_history_20190105 where batch_id=20190207; -- 577490668


INSERT INTO TABLE ascena_staging_dev.jus_hst_promotion_history_dr PARTITION(batch_id=20190101) 
select 
promotion_id
,promoted_entity_id
,promotion_execution_date
,promotion_channel
,original_individual_id
,cell_code
,panel_id
,segment_id
,package_id
,selection_type_code
,model_tile
,email_address_id
,cluster_code
,like_quantity_rank
,control_weighting_factor
,loyalty_store_no
,rfm_score
,crm_id
,last_purchase_date
,customer_status
,new_to_file_reactivated_code
,net_sales_6_mo
,net_sales_7_12_mo
,plcc_status_code
,customer_active_status_code
,closest_store_no
,closest_store_distance
,solicitation_id
,canadian_email_flag
,last_update_date
,last_update_job_id
,uplift_model_score
,seg_cluster_code
FROM ascena_staging_dev.jus_hst_promotion_history_20190105 where batch_id=20190207;


--check the count . It should be sum of both the count
select count(*) from ascena_staging_dev.jus_hst_promotion_history_dr; --1164894942