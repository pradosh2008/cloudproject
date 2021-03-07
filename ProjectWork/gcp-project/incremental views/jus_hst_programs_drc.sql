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