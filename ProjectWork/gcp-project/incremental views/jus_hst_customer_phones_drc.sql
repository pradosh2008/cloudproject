CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_customer_phones_drc
AS
WITH MID AS
(SELECT 
TBL.individual_id
,TBL.phone_no
,MAX(TBL.BATCH_ID) OVER (PARTITION BY TBL.individual_id,TBL.phone_no) AS MAX_BATCH_ID
FROM ASCENA_STAGING_DEV.jus_hst_customer_phones_20190104 TBL)
SELECT tbl1.* FROM ASCENA_STAGING_DEV.jus_hst_customer_phones_20190104 TBL1, MID
where TBL1.BATCH_ID = MID.MAX_BATCH_ID
AND TBL1.individual_id = MID.individual_id
AND TBL1.phone_no = MID.phone_no;
