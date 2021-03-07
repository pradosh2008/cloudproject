--/*********************************************************************
--* 
--* Script Name: DIM_BRAND_CUSTOMER_ADDRESS table creation and data population
--*
--**********************************************************************/


set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER_ADDRESS_MAY (
brand_customer_address_key CHAR(32)
    , brand_customer_key  char(32)
    , brand_cd string
    , zip_postal_cd  string
    , zip_4_cd  string
    , latitude_num DECIMAL(10,6)
    , longitude_num DECIMAL(10,6)
    , mailability_score_num TINYINT
    , return_mail_IND TINYINT
    , return_mail_flagged_dt DATE
    , closest_store_num INT
    , closest_store_dist INT
    , next_closest_store_num INT
    , next_closest_store_dist INT
    , dma_do_not_mail_IND TINYINT
    , foreign_address_IND TINYINT
    , prison_address_IND TINYINT
    , military_address_IND TINYINT
    , last_ncoa_dt DATE
    , ncoa_return_addr_source_CD  string
    , ncoalink_match_CD  string
    , move_effective_dt DATE
    , dsf_business_address_IND TINYINT
    , nursing_home_address_IND TINYINT
    , po_box_IND TINYINT
    , dpv_vacancy_IND TINYINT
    , dsf_seasonal_address_IND TINYINT
    , fips_CD  string
    , carrier_route_CD  string
    , delivery_point_barcode_CD  string
    , lot_CD  string
    , geo_match_level_CD  string
    , last_update_tms TIMESTAMP
    , last_update_job_id INT
    , last_update_source_CD  string
    , address_rec_type_CD  string
    , dpv_footer_CD  string
    , suitelink_ret_CD  string
    , lacs_ret_CD  string
    , check_digit_CD  string
    , lot_order_CD  string
    , jfs_IND TINYINT
    , batch_id BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;



--Brand customer key & Address key make up the primary key
INSERT OVERWRITE TABLE ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER_ADDRESS_MAY
SELECT md5(concat('JUS|',coalesce(a.individual_id,'')
                , '|', coalesce(upper(trim(a.address_1)),'')
                , '|', coalesce(upper(trim(a.address_2)),'')
                , '|', coalesce(upper(trim(a.city)),'')
                , '|', coalesce(upper(trim(a.state_province)),'')
                , '|', coalesce(upper(trim(a.zip_postal_code)),'')
                , '|', coalesce(upper(trim(a.zip_4)),''))
        )                                                           as brand_customer_address_key
, md5(concat('JUS|',coalesce(a.individual_id,''))) as brand_customer_key
, 'JUS' as brand_cd
, a.zip_postal_code as zip_postal_CD
, a.zip_4 as zip_4
, a.latitude as latitude_num
, a.longitude as longitude_num
, a.mailability_score as mailability_score_num
, CASE a.return_mail_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as return_mail_IND
, to_date(from_unixtime(UNIX_TIMESTAMP(a.return_mail_flagged_date,"MM/dd/yyyy"))) as return_mail_flagged_dt
, a.closest_store_no as closest_store_num
, a.closest_store_distance as closest_store_dist
, a.next_closest_store_no as next_closest_store_num
, a.next_closest_store_distance as next_closest_store_dist
, CASE a.dma_do_not_mail_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as dma_do_not_mail_IND
, CASE a.foreign_address_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as foreign_address_IND
, CASE a.prison_address_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as prison_address_IND
, CASE a.military_address_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as military_address_IND
, to_date(from_unixtime(UNIX_TIMESTAMP(a.last_ncoa_date,"MM/dd/yyyy"))) as last_ncoa_dt
, a.ncoa_return_addr_source_code as ncoa_return_addr_source_cd
, a.ncoalink_match_code as ncoalink_match_cd
, to_date(from_unixtime(UNIX_TIMESTAMP(a.move_effective_date,"MM/dd/yyyy"))) as move_effective_dt
, CASE a.dsf_business_address_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as dsf_business_address_IND
, CASE a.nursing_home_address_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as nursing_home_address_IND
, CASE a.po_box_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as po_box_IND
, CASE a.dpv_vacancy_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as dpv_vacancy_IND
, CASE a.dsf_seasonal_address_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as dsf_seasonal_address_IND
, a.fips_code as fips_CD
, a.carrier_route_code as carrier_route_CD
, a.delivery_point_barcode as delivery_point_barcode_CD
, a.lot_code as lot_CD
, a.geo_match_level as geo_match_level_CD
, from_unixtime(UNIX_TIMESTAMP(last_update_date,"MM/dd/yyyy HH:mm:ss")) as last_update_tms
, a.last_update_job_id
, a.last_update_source_code as last_update_source_CD
, a.address_rec_type as address_rec_type_CD
, a.dpv_footer as dpv_footer_CD
, a.suitelink_ret_code as suitelink_ret_CD
, a.lacs_ret_code as lacs_ret_CD
, a.check_digit as check_digit_CD
, a.lot_order as lot_order_CD
, CASE a.jfs_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as jfs_IND
, a.batch_id
FROM ascena_staging_dev.jus_hst_customer_addresses_dr as a where a.batch_id='20190501'
;
