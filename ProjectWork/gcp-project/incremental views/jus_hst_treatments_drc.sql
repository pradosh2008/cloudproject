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