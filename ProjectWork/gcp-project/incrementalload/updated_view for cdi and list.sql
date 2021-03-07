
--create the dedupe view
CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_cdi_key_xref_drc
AS 
WITH MID AS
    (SELECT 
    TBL.original_individual_id
    ,TBL.MAX_BATCH_ID
     from
    (
        SELECT
         original_individual_id as original_individual_id
        ,BATCH_ID AS MAX_BATCH_ID
        ,row_number() over(
        partition by original_individual_id 
                                   order by BATCH_ID desc
                                ) as rn
            from    ascena_staging_dev.jus_hst_cdi_key_xref_dr 
    ) as TBL
    where   rn=1)
SELECT TBL1.* 
    FROM ASCENA_STAGING_DEV.jus_hst_cdi_key_xref_dr  TBL1
JOIN MID
    ON TBL1.BATCH_ID = MID.MAX_BATCH_ID
    AND TBL1.original_individual_id = MID.original_individual_id;
	
	
	
	
CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_lists_drc
AS 
WITH MID AS
    (SELECT 
    TBL.list_id
    ,TBL.MAX_BATCH_ID
     from
    (
        SELECT
         list_id as list_id
        ,BATCH_ID AS MAX_BATCH_ID
        ,row_number() over(
        partition by list_id 
                                   order by BATCH_ID desc
                                ) as rn
            from    ascena_staging_dev.jus_hst_lists_dr 
    ) as TBL
    where   rn=1)
SELECT TBL1.* 
    FROM ASCENA_STAGING_DEV.jus_hst_lists_dr  TBL1
JOIN MID
    ON TBL1.BATCH_ID = MID.MAX_BATCH_ID
    AND TBL1.list_id = MID.list_id;
	
	
CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_lists_drc
AS 
WITH MID AS
    (SELECT 
    TBL.store_no
	,TBL.register_no
	,TBL.transaction_number
	,TBL.ship_date
    ,TBL.MAX_BATCH_ID
     from
    (
        SELECT
         store_no as store_no
		 ,register_no as register_no
		 ,transaction_number as transaction_number
		 ,ship_date as ship_date
        ,BATCH_ID AS MAX_BATCH_ID
        ,row_number() over(
        partition by list_id 
                                   order by BATCH_ID desc
                                ) as rn
            from    ascena_staging_dev.jus_hst_lists_dr 
    ) as TBL
    where   rn=1)
SELECT TBL1.* 
    FROM ASCENA_STAGING_DEV.jus_hst_lists_dr  TBL1
JOIN MID
    ON TBL1.BATCH_ID = MID.MAX_BATCH_ID
    AND TBL1.list_id = MID.list_id;
	
	
CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_transactions_drc
AS
WITH MID AS
(SELECT 
--MAX(TBL.BATCH_ID) OVER (PARTITION BY tbl.store_no,tbl.register_no,tbl.transaction_number,tbl.ship_date) AS MAX_BATCH_ID
MAX(TBL.BATCH_ID) as MAX_BATCH_ID
FROM ASCENA_STAGING_DEV.jus_hst_transactions_20190104 TBL)
SELECT * FROM ASCENA_STAGING_DEV.jus_hst_transactions_20190104 TBL1, MID
where TBL1.BATCH_ID = MID.MAX_BATCH_ID;
