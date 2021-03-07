--/*********************************************************************
--
-- Script Name: promo_cust_reference table creation and data population
--
--**********************************************************************/


set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


create TABLE IF NOT EXISTS ascena_analytic_mart_dev.promo_cust_reference_may
(
promo_cust_reference_key char(32)
,promotion_key char(32)
,brand_customer_key char(32)
,promoted_entity_id           bigint
,promotion_execution_dt date
,PROMOTION_CHANNEL_CD string
,original_individual_id bigint
,cell_cd string
,panel_id  string
,segment_id bigint
,package_id   bigint
,selection_type_cd string
,model_tile_num bigint
,email_address_id             bigint
,cluster_cd bigint
,like_quantity_rank_num bigint
,control_weighting_fctr decimal(15,10)
,loyalty_store_num bigint
,rfm_score_num bigint
,crm_id                       bigint
,last_purchase_dt date
,customer_status_cd string
,new_to_file_reactivated_cd string
,net_sales_6_mo_amt decimal(25,9)
,net_sales_7_12_mo_amt   decimal(25,9)
,plcc_status_cd string
,customer_active_status_cd string
,closest_store_num bigint
,closest_store_distance_num bigint
,solicitation_id              bigint
,canadian_email_ind TINYINT
,last_update_tms            timestamp
,last_update_job_id           bigint
,uplift_model_score_num string
,seg_cluster_cd bigint
,batch_id bigint
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;
                    

INSERT OVERWRITE TABLE ascena_analytic_mart_dev.promo_cust_reference_may 
select
md5(concat('JUS|',coalesce(a.promotion_id,''),'|',coalesce(a.promoted_entity_id,''),'|',coalesce(from_unixtime(UNIX_TIMESTAMP(a.promotion_execution_date,"MM/dd/yyyy HH:mm:ss"), "yyyyMMdd HH:mm:ss"),''))) as promo_cust_reference_key
, md5(concat('JUS|',coalesce(a.promotion_id,''))) as  promotion_key
, CASE WHEN c.individual_id is not null THEN md5(concat('JUS|',coalesce(c.individual_id,''))) ELSE '-1' END as brand_customer_key
, a.promoted_entity_id as	promoted_entity_id
, to_date(from_unixtime(UNIX_TIMESTAMP(a.promotion_execution_date,"MM/dd/yyyy"))) as promotion_execution_dt
, a.promotion_channel as	promotion_channel_cd
, a.original_individual_id as	original_individual_id
, a.cell_code as	cell_cd
, a.panel_id as	panel_id 
, a.segment_id as	segment_id
, a.package_id as	package_id
, a.selection_type_code as	selection_type_cd
, a.model_tile as	model_tile_num
, a.email_address_id as	email_address_id
, a.cluster_code as	cluster_cd
, a.like_quantity_rank as	like_quantity_rank_num
, a.control_weighting_factor as	control_weighting_fctr
, a.loyalty_store_no as	loyalty_store_num
, a.rfm_score as	rfm_score_num
, a.crm_id as	crm_id
, to_date(from_unixtime(UNIX_TIMESTAMP(a.last_purchase_date,"MM/dd/yyyy"))) as	last_purchase_dt
, a.customer_status as	customer_status_cd
, a.new_to_file_reactivated_code as	new_to_file_reactivated_cd
, a.net_sales_6_mo as	net_sales_6_mo_amt
, a.net_sales_7_12_mo as	net_sales_7_12_mo_amt
, a.plcc_status_code as	plcc_status_cd
, a.customer_active_status_code as	customer_active_status_cd
, a.closest_store_no as	closest_store_num
, a.closest_store_distance as	closest_store_distance_num
, a.solicitation_id as	solicitation_id
, CASE WHEN a.canadian_email_flag ='Y' THEN 1 WHEN a.canadian_email_flag='N' THEN 0 ELSE 0 END as canadian_email_ind
, from_unixtime(UNIX_TIMESTAMP(a.last_update_date,"MM/dd/yyyy HH:mm:ss")) as last_update_tms
, a.last_update_job_id as	last_update_job_id
, a.uplift_model_score as	uplift_model_score_num
, a.seg_cluster_code as	seg_cluster_cd
,20190501 as batch_id
from (select b.original_individual_id 
            , b.current_individual_id
			, c.individual_id
    from ascena_staging_DEV.jus_hst_cdi_key_xref_dr b  
    left join ascena_staging_DEV.jus_hst_customers_dr c
        on b.current_individual_id=c.individual_id
    where b.batch_id='20190501'
) xref
join ascena_staging_dev.jus_hst_promotion_history_dr a
    on a.original_individual_id=xref.original_individual_id
	
