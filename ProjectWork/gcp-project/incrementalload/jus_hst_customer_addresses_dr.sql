-- Run this two command as we are dynamically loading partitions.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- check the count before insertion
select batch_id from ascena_staging_dev. jus_hst_customer_addresses_20190104 group by batch_id;---20190204

select count(*) from ascena_staging_dev.jus_hst_customer_addresses_dr; --44254023
select count(*) from ascena_staging_dev.  jus_hst_customer_addresses_20190104; -- 43948672

desc ascena_staging_dev.jus_hst_customer_addresses_dr;
desc ascena_staging_dev. jus_hst_customer_addresses_20190104;


INSERT INTO TABLE ascena_staging_dev.jus_hst_customer_addresses_dr PARTITION(batch_id=20190101) 
select 
individual_id                
,address_1                    
,address_2                    
,city                         
,state_province               
,zip_postal_code              
,zip_4                        
,country                      
,latitude                     
,longitude                    
,mailability_score            
,return_mail_flag             
,return_mail_flagged_date     
,closest_store_no             
,closest_store_distance       
,next_closest_store_no        
,next_closest_store_distance  
,dma_do_not_mail_flag         
,foreign_address_flag         
,prison_address_flag          
,military_address_flag        
,last_ncoa_date               
,ncoa_return_addr_source_code 
,ncoalink_match_code          
,move_effective_date          
,dsf_business_address_flag    
,nursing_home_address_flag    
,po_box_flag                  
,dpv_vacancy_flag             
,dsf_seasonal_address_flag    
,fips_code                    
,carrier_route_code           
,delivery_point_barcode       
,lot_code                     
,geo_match_level              
,last_update_date             
,last_update_job_id           
,last_update_source_code      
,address_rec_type             
,dpv_footer                   
,suitelink_ret_code           
,lacs_ret_code                
,check_digit                  
,lot_order                    
,jfs_flag                            
FROM ascena_staging_dev. jus_hst_customer_addresses_20190104 where batch_id=20190204;

--check the count . It should be sum of both the count
select count(*) from ascena_staging_dev.jus_hst_customer_addresses_dr; --88202695


CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_customer_addresses_drc
AS 
WITH MID AS
(SELECT 
TBL.INDIVIDUAL_ID
,TBL.ADDRESS_1
,TBL.ADDRESS_2
,TBL.CITY
,TBL.STATE_PROVINCE
,TBL.ZIP_POSTAL_CODE
,TBL.ZIP_4
,TBL.MAX_BATCH_ID     
from
(
SELECT
INDIVIDUAL_ID
,ADDRESS_1
,ADDRESS_2
,CITY
,STATE_PROVINCE
,ZIP_POSTAL_CODE
,ZIP_4
,BATCH_ID AS MAX_BATCH_ID
,row_number() over(
partition by INDIVIDUAL_ID,ADDRESS_1,ADDRESS_2,CITY,STATE_PROVINCE,ZIP_POSTAL_CODE,ZIP_4
order by BATCH_ID desc
) as rn
from    ascena_staging_dev.jus_hst_customer_addresses_dr 
) as TBL
where   rn=1)
SELECT TBL1.* 
FROM ASCENA_STAGING_DEV.jus_hst_customer_addresses_dr  TBL1
JOIN MID
ON TBL1.BATCH_ID = MID.MAX_BATCH_ID
AND TBL1.INDIVIDUAL_ID = MID.INDIVIDUAL_ID
AND TBL1.ADDRESS_1 = MID.ADDRESS_1
AND TBL1.ADDRESS_2 = MID.ADDRESS_2
AND TBL1.CITY = MID.CITY
AND TBL1.STATE_PROVINCE = MID.STATE_PROVINCE
AND TBL1.ZIP_POSTAL_CODE = MID.ZIP_POSTAL_CODE
AND TBL1.ZIP_4 = MID.ZIP_4;


SELECT COUNT(*) FROM ASCENA_STAGING_DEV.jus_hst_customer_addresses_drc;


