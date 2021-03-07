--/*********************************************************************
--* 
--* Script Name: PROMO_OFFER_LINKAGE table creation and data population
--*
--**********************************************************************/
--/*
--md5(concat('JUS|',coalesce(HPO.PROMOTION_ID, ''), coalesce(HPS.SEGMENT_ID, ''), coalesce(HPS.PROMOTION_EXECUTION_ID, ''), trim(coalesce(HPC.CELL_CODE, '')), coalesce(HP.PACKAGE_ID, ''), trim(coalesce(HPT.TREATMENT_CODE, '')), coalesce(HO.OFFER_CODE_ID, '') )) AS PROMO_OFFER_REFERENCE_KEY


set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.PROMO_OFFER_LINKAGE_MAY (
PROMO_OFFER_REFERENCE_KEY CHAR(32)
, PROMO_EXEC_HIER_KEY CHAR(32)
, PACKAGE_TREATMENT_KEY CHAR(32)
, OFFER_KEY CHAR(32)
, TREATMENT_KEY CHAR(32)
, PROMOTION_KEY CHAR(32)
, PROMOTION_ID BIGINT
, PROMOTION_NM STRING
, SEGMENT_ID BIGINT
, SEGMENT_DES STRING
, PROMOTION_EXECUTION_ID BIGINT
, CELL_CD STRING
, CELL_DES STRING
, PACKAGE_ID BIGINT
, PACKAGE_DES STRING
, TREATMENT_CD STRING
, TREATMENT_DES STRING
, OFFER_CODE_ID BIGINT
, OFFER_DES STRING
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

INSERT OVERWRITE TABLE ascena_analytic_mart_dev.promo_offer_linkage_may
SELECT md5(concat('JUS|',coalesce(HPO.PROMOTION_ID, ''), '|', coalesce(HPS.SEGMENT_ID, ''),  '|', coalesce(HPS.PROMOTION_EXECUTION_ID, ''),  '|', coalesce(upper(trim(HPC.CELL_CODE)), ''),  '|', coalesce(HP.PACKAGE_ID, ''), '|', coalesce(upper(trim(HT.TREATMENT_CODE)), ''),  '|', coalesce(HO.OFFER_CODE_ID, '') )) AS PROMO_OFFER_REFERENCE_KEY
, md5(concat('JUS|', coalesce(HPS.promotion_id, ''), '|', coalesce(HPS.segment_id, ''), '|', coalesce(upper(trim(HPC.cell_code)), ''), '|', coalesce(HPS.promotion_execution_id, ''))) as PROMO_EXEC_HIER_KEY
, md5(concat('JUS|', coalesce(HPT.package_id, ''), '|', coalesce(upper(trim(HPT.treatment_code)), ''))) as PACKAGE_TREATMENT_KEY
, md5(concat('JUS|',COALESCE(HO.offer_code_id, CAST(0 AS BIGINT)), '|', coalesce(upper(trim(HO.offer_code)), ''))) as OFFER_KEY
, md5(concat('JUS|', coalesce(upper(trim(HT.treatment_code)), ''))) as TREATMENT_KEY
, md5(concat('JUS|', coalesce(HPO.PROMOTION_ID, ''))) as PROMOTION_KEY
, HPO.PROMOTION_ID as PROMOTION_ID
, HPO.promotion_name as promotion_nm
, HPS.SEGMENT_ID as SEGMENT_ID
, HPS.segment_description as segment_des
, HPS.PROMOTION_EXECUTION_ID as PROMOTION_EXECUTION_ID
, HPC.CELL_CODE as CELL_CD
, HPC.cell_description as cell_des
, HP.PACKAGE_ID as PACKAGE_ID
, HP.package_description as package_des
, HT.TREATMENT_CODE as TREATMENT_CD
, HT.treatment_description as treatment_des
, HO.OFFER_CODE_ID as OFFER_CODE_ID
, HO.offer_desc as offer_des
FROM ascena_staging_dev.jus_hst_promotions_dr AS HPO
LEFT JOIN  ascena_staging_dev.JUS_HST_PROMOTION_SEGMENTS_DR AS HPS
	ON HPO.PROMOTION_ID = HPS.PROMOTION_ID
LEFT JOIN ascena_staging_dev.JUS_HST_PROMOTION_CELLS_DR AS HPC 
	ON HPS.PROMOTION_ID = HPC.PROMOTION_ID AND HPS.SEGMENT_ID = HPC.SEGMENT_ID AND HPS.PROMOTION_EXECUTION_ID = HPC.PROMOTION_EXECUTION_ID
LEFT JOIN ascena_staging_dev.JUS_HST_PACKAGES_DR AS HP
	ON HP.PACKAGE_ID = HPC.PACKAGE_ID 
LEFT JOIN ascena_staging_dev.jus_hst_package_treatments_dr AS HPT
	ON HP.PACKAGE_ID  = HPT.PACKAGE_ID 
LEFT JOIN ascena_staging_dev.JUS_HST_TREATMENTS_DR AS HT
	ON HT.TREATMENT_CODE = HPT.TREATMENT_CODE
LEFT JOIN ascena_staging_dev.jus_hst_offer_codes_dr AS HO
	ON HT.OFFER_CODE_ID = HO.OFFER_CODE_ID
where HPO.batch_id='20190501' 
and HPS.batch_id='20190501' 
and HPC.batch_id='20190501' 
and HP.batch_id='20190501' 
and HPT.batch_id='20190501' 
and HT.batch_id='20190501' 
and HO.batch_id='20190501';
