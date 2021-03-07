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