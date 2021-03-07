#loading the temporary table in staging (it has both new and updated records)
bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_stage.pre_store --use_legacy_sql=false <<!
SELECT
FARM_FINGERPRINT(CAST(ls.store_nbr AS STRING)) as store_key
, CAST(ls.store_nbr AS STRING) as store_id
, CASE WHEN distribution_channel_cd = '05' AND region_cd = '0030' THEN 'LNG' ELSE ls.brand_cd END as brand_des 
, CASE WHEN ls.brand_cd = 'ENT' THEN '99' ELSE ss.brand END as brand_cd
, CASE  WHEN distribution_channel_cd IN ('05','06') AND region_cd = '0030' THEN '65'  -- Distribution Channel 05 and 06 for Brand_cd 'L'
		WHEN distribution_channel_cd IN ('05','06') AND region_cd = '0613' THEN '65'  -- Distribution Channel 05 and 06 for Brand_cd 'L'
        WHEN distribution_channel_cd IN ('02','03') THEN '63'
        WHEN distribution_channel_cd IN ('04','08') THEN '66'
	    WHEN distribution_channel_cd IN ('05','06') THEN '64' 
        WHEN distribution_channel_cd IN ('07','09') THEN '64'
        ELSE NULL END as chain_id
, ls.store_status_cd
, CASE WHEN ls.store_status_cd = 'C' THEN 'CLOSED' WHEN ls.store_status_cd = 'O' THEN 'OPEN' ELSE NULL END as store_status_des
, ls.store_nm as store_nam
, ls.store_address_1 as store_address_1_des
, ls.store_address_2 as store_address_2_des
, ls.store_city as store_city_nam
, ls.store_state as store_state_cd
, ls.store_postal_cd as store_postal_cd
, ls.store_country_cd as store_country_cd
, ls.store_latitude as store_latitude_num
, ls.store_longitude as store_longitude_num
, ls.region_cd as region_cd
, ls.region_desc as region_des
, ls.district_cd as district_cd
, ls.district_desc as district_des
, ls.tax_jurisdiction as tax_jurisdiction_num
, ls.title_cd as title_cd
, ls.title_desc as title_des
, ls.title_person as title_person_nam
, ls.regional_vp_nm as regional_vp_nam
, ls.district_mgr_nm as district_manager_nam
, ls.store_mgr_nm as store_manager_nam
, ls.total_area as total_area_num
, ls.sales_area as sales_area_num
, ss.sales_area_unit AS sales_area_unit
, ss.search_term2 as selling_des
, ls.selling_ind
, CAST(ls.store_open_dt AS DATE) as store_open_dt
, CAST(ls.store_close_dt AS DATE) as store_close_dt
, ls.climate as climate_des
, ls.location as location_des
, ls.secondary_location as secondary_location_des
, ls.rov as rov_des
, ls.tier as tier_num
, ls.volume as volume_cd
, ls.sales_org_cd as sales_org_cd
, ls.company_cd
, ls.distribution_channel_cd
, ls.store_phone_nbr as store_phone_num
, 20160101 as batch_id  
FROM edl_stage.pre_lu_store_vw as ls
LEFT OUTER JOIN ( SELECT * FROM edl_landing.ann_ann_sap_store WHERE store_no NOT LIKE 'A%' AND store_no NOT LIKE 'R%')
ss ON (ls.store_nbr = CAST(ss.store_no AS INT64))
UNION ALL
SELECT
FARM_FINGERPRINT(CAST(CAST(store_no AS INT64) AS STRING)) as store_key
, CAST(CAST(store_no AS INT64) AS STRING) as store_id
, CASE WHEN distribution_channel = '05' AND region = '0030' THEN 'LNG'
       WHEN distribution_channel IN ('02','03') THEN 'AT'
       WHEN distribution_channel IN ('04','08') THEN 'ATF'
       WHEN distribution_channel IN ('05','06') THEN 'LOFT'
       WHEN distribution_channel IN ('07','09') THEN 'LOS'
       WHEN brand = '99' THEN 'ENT'
       ELSE NULL END as brand_des
, brand as brand_cd
, CASE  WHEN distribution_channel IN ('05','06') AND region = '0030' THEN '65'  -- Distribution Channel 05 and 06 for Brand_cd 'L'
		WHEN distribution_channel IN ('05','06') AND region = '0613' THEN '65'  -- Distribution Channel 05 and 06 for Brand_cd 'L'
        WHEN distribution_channel IN ('02','03') THEN '63'
        WHEN distribution_channel IN ('04','08') THEN '66'
	    WHEN distribution_channel IN ('05','06') THEN '64' 
        WHEN distribution_channel IN ('07','09') THEN '64'
        ELSE NULL END as chain_id
, CASE WHEN TRIM(search_term1) = 'OPEN' THEN 'O' WHEN TRIM(search_term1) LIKE 'CLOSED%' THEN 'C' ELSE NULL END as store_status_cd
, search_term1 as store_status_des
, store_name as store_nam
, street as store_address_1
, NULL as store_address_2
, city as store_city
, state as store_state_cd
, zipcode as store_postal_cd
, country as store_country_cd
, NULL as store_latitude
, NULL as store_longitude
, region as region_cd
, region_desc as region_des
, district as district_cd
, district_desc as district_des
, tax_jurisdiction as tax_jurisdiction_num
, title_value as title_cd
, title_desc as title_des
, name as title_person
, regional_vise_president as regional_vp_nam
, district_manager as district_manager_nam
, store_manager as store_manager_nam
, CAST(tot_sq_ft AS INT64)as total_area_num
, CAST(sales_area AS INT64) as sales_area_num
, sales_area_unit as sales_area_unit_cd
, search_term2 as selling_des
, CASE WHEN search_term2 = 'SELLING' THEN 1 ELSE 0 END AS selling_ind
, CASE WHEN CAST(opening_date AS INT64) = 0 THEN NULL ELSE PARSE_DATE('%Y%m%d',opening_date) END as store_open_dt
, CASE WHEN CAST(closing_date AS INT64) = 0 THEN NULL ELSE PARSE_DATE('%Y%m%d',closing_date) END as store_close_dt
, climate as climate_des
, location as location_des
, secondary_location as secondary_location_des
, rov as rov_des
, tier as tier_num
, volume as volume_cd 
, sales_org as sales_org_cd
, company_code as company_cd
, distribution_channel as distribution_channel_cd
, store_time_zone as store_phone_nbr
, batch_id  
FROM edl_stage.pre_sap_store
WHERE store_no NOT LIKE 'A%' AND store_no NOT LIKE 'R%'
AND CAST(store_no AS INT64) NOT IN (SELECT store_nbr FROM edl_stage.pre_lu_store_vw)
UNION ALL
SELECT
FARM_FINGERPRINT(store_no) as store_key
, store_no as store_id
, CASE WHEN distribution_channel = '05' AND region = '0030' THEN 'LNG'
       WHEN distribution_channel IN ('02','03') THEN 'AT'
       WHEN distribution_channel IN ('04','08') THEN 'ATF'
       WHEN distribution_channel IN ('05','06') THEN 'LOFT'
       WHEN distribution_channel IN ('07','09') THEN 'LOS'
       WHEN brand = '99' THEN 'ENT'
       ELSE NULL END as brand_des
, brand as brand_cd
, CASE  WHEN distribution_channel IN ('05','06') AND region = '0030' THEN '65'  -- Distribution Channel 05 and 06 for Brand_cd 'L'
		WHEN distribution_channel IN ('05','06') AND region = '0613' THEN '65'  -- Distribution Channel 05 and 06 for Brand_cd 'L'
        WHEN distribution_channel IN ('02','03') THEN '63'
        WHEN distribution_channel IN ('04','08') THEN '66'
	    WHEN distribution_channel IN ('05','06') THEN '64' 
        WHEN distribution_channel IN ('07','09') THEN '64'
        ELSE NULL END as chain_id
, CASE WHEN TRIM(search_term1) = 'OPEN' THEN 'O' WHEN TRIM(search_term1) LIKE 'CLOSED%' THEN 'C' ELSE NULL END as store_status_cd
, search_term1 as store_status_des
, store_name as store_nam
, street as store_address_1
, NULL as store_address_2
, city as store_city
, state as store_state_cd
, zipcode as store_postal_cd
, country as store_country_cd
, NULL as store_latitude
, NULL as store_longitude
, region as region_cd
, region_desc as region_des
, district as district_cd
, district_desc as district_des
, tax_jurisdiction as tax_jurisdiction_num
, title_value as title_cd
, title_desc as title_des
, name as title_person
, regional_vise_president as regional_vp_nam
, district_manager as district_manager_nam
, store_manager as store_manager_nam
, CAST(tot_sq_ft AS INT64)as total_area_num
, CAST(sales_area AS INT64) as sales_area_num
, sales_area_unit as sales_area_unit_cd
, search_term2 as selling_des
, CASE WHEN search_term2 = 'SELLING' THEN 1 ELSE 0 END AS selling_ind
, CASE WHEN CAST(opening_date AS INT64) = 0 THEN NULL ELSE PARSE_DATE('%Y%m%d',opening_date) END as store_open_dt
, CASE WHEN CAST(closing_date AS INT64) = 0 THEN NULL ELSE PARSE_DATE('%Y%m%d',closing_date) END as store_close_dt
, climate as climate_des
, location as location_des
, secondary_location as secondary_location_des
, rov as rov_des
, tier as tier_num
, volume as volume_cd 
, sales_org as sales_org_cd
, company_code as company_cd
, distribution_channel as distribution_channel_cd
, store_time_zone as store_phone_nbr
, batch_id  
FROM edl_stage.pre_sap_store
WHERE store_no LIKE 'A%' OR store_no LIKE 'R%'
!

#load edl_stage.pre_store to temporary table for incremental load
bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table analytic_mart.pre_store_new --use_legacy_sql=false <<!
SELECT * FROM edl_stage.pre_store
!

#append the old records into the temporary table
bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --append_table --destination_table analytic_mart.pre_store_new --use_legacy_sql=false <<!
select c.*
from analytic_mart.pre_store c
left join edl_stage.pre_store w
    on  w.store_id = c.store_id
        where w.store_id is null
!

#cleansing and archival
bq cp --force analytic_mart.pre_store edl_archive.pre_store
bq cp --force analytic_mart.pre_store_new edl_stage.pre_store
bq rm --force analytic_mart.pre_store_new
bq rm --force edl_stage.pre_store
