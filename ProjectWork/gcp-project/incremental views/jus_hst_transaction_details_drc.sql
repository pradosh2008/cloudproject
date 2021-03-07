CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_transaction_details_drc
AS
WITH MID AS
(SELECT 
--MAX(TBL.BATCH_ID) OVER (PARTITION BY tbl.store_no,tbl.register_no,tbl.transaction_number,tbl.ship_date,tbl.line_no) AS MAX_BATCH_ID
MAX(TBL.BATCH_ID) as MAX_BATCH_ID
FROM ASCENA_STAGING_DEV.jus_hst_transaction_details_20190105 TBL)
SELECT * FROM ASCENA_STAGING_DEV.jus_hst_transaction_details_20190105 TBL1, MID
where TBL1.BATCH_ID = MID.MAX_BATCH_ID;
