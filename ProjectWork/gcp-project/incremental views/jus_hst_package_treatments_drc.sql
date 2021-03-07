CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.JUS_HST_PACKAGE_TREATMENTS_DRC
AS 
WITH MID AS
(SELECT 
TBL.PACKAGE_ID
,TBL.TREATMENT_CODE
,MAX(TBL.BATCH_ID) OVER (PARTITION BY TBL.PACKAGE_ID,TBL.TREATMENT_CODE) AS MAX_BATCH_ID
FROM ASCENA_STAGING_DEV.JUS_HST_PACKAGE_TREATMENTS_20190105 TBL)
SELECT TBL1.* 
FROM ASCENA_STAGING_DEV.JUS_HST_PACKAGE_TREATMENTS_20190105 TBL1
JOIN MID
ON TBL1.BATCH_ID = MID.MAX_BATCH_ID	
AND TBL1.PACKAGE_ID = MID.PACKAGE_ID
AND TBL1.TREATMENT_CODE = MID.TREATMENT_CODE;