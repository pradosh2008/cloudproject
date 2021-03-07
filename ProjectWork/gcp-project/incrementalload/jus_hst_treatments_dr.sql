-- Run this two command as we are dynamically loading partitions.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- check the count before insertion
select count(*) from ascena_staging_dev.jus_hst_treatments_dr; -- 887
select batch_id from ASCENA_STAGING_DEV.JUS_HST_TREATMENTS_20190105 group by batch_id; --20190207
select count(*) from ascena_staging_dev.JUS_HST_TREATMENTS_20190105 where batch_id=20190207; -- 861



-- Insert data into the table
INSERT INTO TABLE ascena_staging_dev.jus_hst_treatments_dr PARTITION(batch_id=20190101) 
select 
treatment_code           
,treatment_type           
,treatment_description    
,active_flag              
,offer_code_id            
,treatment_offer_code     
,offer_start_date         
,offer_end_date           
,offer_type_code          
,offer_desc               
,offer_status_code        
,offer_category           
,last_update_date         
,last_update_user_id                                             
FROM ascena_staging_dev.JUS_HST_TREATMENTS_20190105 where batch_id=20190207;

--check the count . It should be sum of both the count
select count(*) from ascena_staging_dev.jus_hst_treatments_dr; --1748


--create the dedupe view
CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.JUS_HST_TREATMENTS_DRC
AS 
WITH MID AS
    (SELECT 
    TBL.TREATMENT_CODE
    ,TBL.MAX_BATCH_ID
     from
    (
        SELECT
         TREATMENT_CODE as TREATMENT_CODE
        ,BATCH_ID AS MAX_BATCH_ID
        ,row_number() over(
        partition by TREATMENT_CODE 
                                   order by BATCH_ID desc
                                ) as rn
            from    ascena_staging_dev.jus_hst_treatments_dr 
    ) as TBL
    where   rn=1)
SELECT TBL1.* 
    FROM ASCENA_STAGING_DEV.jus_hst_treatments_dr  TBL1
JOIN MID
    ON TBL1.BATCH_ID = MID.MAX_BATCH_ID
    AND TBL1.TREATMENT_CODE = MID.TREATMENT_CODE;
  
--check the count of the view 
select count(*) from ASCENA_STAGING_DEV.JUS_HST_TREATMENTS_DRC; --887
