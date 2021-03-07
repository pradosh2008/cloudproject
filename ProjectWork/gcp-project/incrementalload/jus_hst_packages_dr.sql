-- Run this two command as we are dynamically loading partitions.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- check the count before insertion
select count(*) from ascena_staging_dev.jus_hst_packages_dr; -- 637
select batch_id from ASCENA_STAGING_DEV.JUS_HST_PACKAGES_20190105 group by batch_id; --20190205
select count(*) from ascena_staging_dev.JUS_HST_PACKAGES_20190105 where batch_id=20190205; -- 625

describe ascena_staging_dev.jus_hst_packages_dr;
describe ascena_staging_dev.JUS_HST_PACKAGES_20190105;

-- Insert data into the table
INSERT INTO TABLE ascena_staging_dev.jus_hst_packages_dr PARTITION(batch_id=20190101) 
select 
 package_id               
 ,package_description      
 ,cost                     
 ,creative_cost            
 ,postage_cost             
 ,production_cost          
 ,other_cost               
 ,language                 
 ,last_update_date         
 ,last_update_user_id                                             
FROM ascena_staging_dev.JUS_HST_PACKAGES_20190105 where batch_id=20190205;

--check the count . It should be sum of both the count
select count(*) from ascena_staging_dev.jus_hst_packages_dr; --1262


--create the dedupe view
CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.JUS_HST_PACKAGES_DRC
AS 
WITH MID AS
    (SELECT 
    TBL.PACKAGE_ID
    ,TBL.MAX_BATCH_ID
     from
    (
        SELECT
         PACKAGE_ID as PACKAGE_ID
        ,BATCH_ID AS MAX_BATCH_ID
        ,row_number() over(
        partition by PACKAGE_ID 
                                   order by BATCH_ID desc
                                ) as rn
            from    ascena_staging_dev.jus_hst_packages_dr 
    ) as TBL
    where   rn=1)
SELECT TBL1.* 
    FROM ASCENA_STAGING_DEV.jus_hst_packages_dr  TBL1
JOIN MID
    ON TBL1.BATCH_ID = MID.MAX_BATCH_ID
    AND TBL1.PACKAGE_ID = MID.PACKAGE_ID;
  
--check the count of the view 
select count(*) from ASCENA_STAGING_DEV.JUS_HST_PACKAGES_DRC; -- 637
