-- Run this two command as we are dynamically loading partitions.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- check the count before insertion
select count(*) from ascena_staging_dev.jus_hst_customer_email_addresses_dr; --35,393,220
select count(*) from ascena_staging_dev.jus_hst_customer_email_addresses_20190104; -- 35,142,696


INSERT INTO TABLE ascena_staging_dev.jus_hst_customer_email_addresses_dr PARTITION(batch_id=20190101) 
select 
email_address                   
,individual_id                   
,soft_bounce_count               
,bounced_flag                    
,invalid_flag                    
,email_preference_change_time    
,create_date                     
,create_source_code              
,create_job_id                   
,last_update_time                
,last_update_job_id              
,email_preference_code           
,last_update_source_code         
,email_address_id                
,coppa_flag                      
,fra_hygiene_cd                  
,fra_suggested_email             
,fra_comment                     
,fra_update_date                 
,complaint_flag                  
,subscr_status_justice           
,subscr_status_justice_date      
,subscr_status_bro               
,subscr_status_bro_date          
,subscr_status_justice_can       
,subscr_status_justice_can_date  
,et_rejected                     
,riid                            
FROM ascena_staging_dev.jus_hst_customer_email_addresses_20190104 where batch_id=20190204;


--check the count . It should be sum of both the count
select count(*) from ascena_staging_dev.jus_hst_customer_email_addresses_dr; --70,535,916


CREATE 
    OR 
REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_customer_email_addresses_drc
AS
WITH MID AS
(
    select  TBL.individual_id
     ,TBL.email_address
     ,TBL.MAX_BATCH_ID 
    from
    (
        SELECT
         individual_id as individual_id
        ,email_address as email_address
        ,BATCH_ID AS MAX_BATCH_ID
        ,row_number() over(
        partition by individual_id, email_address 
                                   order by BATCH_ID desc
                                ) as rn
            from    ascena_staging_dev.jus_hst_customer_email_addresses_dr
    ) as TBL
    where   rn=1)
SELECT  TBL1.* 
FROM    ASCENA_STAGING_DEV.jus_hst_customer_email_addresses_dr TBL1,
        MID
where   TBL1.BATCH_ID = MID.MAX_BATCH_ID
    AND TBL1.individual_id = MID.individual_id
    AND TBL1.email_address = MID.email_address ;

select count(*) from ASCENA_STAGING_DEV.jus_hst_customer_email_addresses_drc; --39,568,722