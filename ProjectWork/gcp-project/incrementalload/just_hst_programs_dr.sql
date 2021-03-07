-- Run this two command as we are dynamically loading partitions.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- check the count before insertion
select count(*) from ascena_staging_dev.jus_hst_programs_dr; -- 124
select batch_id from ascena_staging_dev.jus_hst_programs_20190105 group by batch_id; --20190205
select count(*) from ascena_staging_dev.jus_hst_programs_20190105 where batch_id=20190205; -- 121

-- Insert data into the table
INSERT INTO TABLE ascena_staging_dev.jus_hst_programs_dr PARTITION(batch_id=20190101) 
select 
 program_id               
 ,program_type             
 ,business_unit_code       
 ,program_code             
 ,program_name             
 ,program_description      
 ,program_status           
 ,ly_program_code          
 ,last_update_user_id      
 ,last_update_date                                       
FROM ascena_staging_dev.jus_hst_programs_20190105 where batch_id=20190205;

--check the count . It should be sum of both the count
select count(*) from ascena_staging_dev.jus_hst_programs_dr; --245


--create the dedupe view
CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.JUS_HST_PROGRAMS_DRC
AS 
WITH MID AS
    (SELECT 
    TBL.PROGRAM_ID
    ,TBL.MAX_BATCH_ID
     from
    (
        SELECT
         PROGRAM_ID as PROGRAM_ID
        ,BATCH_ID AS MAX_BATCH_ID
        ,row_number() over(
        partition by PROGRAM_ID 
                                   order by BATCH_ID desc
                                ) as rn
            from    ascena_staging_dev.jus_hst_programs_dr 
    ) as TBL
    where   rn=1)
SELECT TBL1.* 
    FROM ASCENA_STAGING_DEV.jus_hst_programs_dr  TBL1
JOIN MID
    ON TBL1.BATCH_ID = MID.MAX_BATCH_ID
    AND TBL1.PROGRAM_ID = MID.PROGRAM_ID;
  
--check the count of the view 
select count(*) from ASCENA_STAGING_DEV.JUS_HST_PROGRAMS_DRC; --124