CREATE OR REPLACE TABLE analytic_mart.pre_store AS 
SELECT
CAST(store_no AS INT64) as store_key
, CASE WHEN ss.brand = hs.brand_cd THEN ss.brand ELSE hs.brand_cd END as brand_cd
, ss.search_term1 as store_status_des
, hs.store_status_cd as store_status_cd
, CASE WHEN ss.store_name = hs.store_nm THEN ss.store_name ELSE hs.store_nm END as store_nam
, ss.street as store_address_1
, hs.store_address_2 as store_address_2
, CASE WHEN ss.city = hs.store_city THEN ss.city ELSE hs.store_city END as store_city
, CASE WHEN ss.state = hs.store_state THEN ss.state ELSE hs.store_state END as store_state
, CASE WHEN ss.zipcode = hs.store_postal_cd THEN ss.zipcode ELSE hs.store_postal_cd END as store_postal_cd
, CASE WHEN ss.country = hs.store_country_cd THEN ss.country ELSE hs.store_country_cd END as store_country_cd
, hs.store_latitude AS store_latitude
, hs.store_longitude AS store_longitude
, CASE WHEN ss.region = hs.region_cd THEN ss.region ELSE hs.region_cd END as region_cd
, CASE WHEN ss.region_desc = hs.region_desc THEN ss.region_desc ELSE hs.region_desc END as region_des
, CASE WHEN ss.district = hs.district_cd THEN ss.district ELSE hs.district_cd END as district_cd
, CASE WHEN ss.district_desc = hs.district_desc THEN ss.district_desc ELSE hs.district_desc END as district_des
, CASE WHEN ss.tax_jurisdiction = hs.tax_jurisdiction THEN ss.tax_jurisdiction ELSE hs.tax_jurisdiction END as tax_jurisdiction
, CASE WHEN ss.title_value = hs.title_cd THEN ss.title_value ELSE hs.title_cd END as title_cd
, CASE WHEN ss.title_desc = hs.title_desc THEN ss.title_desc ELSE hs.title_desc END as title_des
, CASE WHEN ss.name = hs.title_person THEN ss.name ELSE hs.title_person END as title_person_nam
, CASE WHEN ss.regional_vise_president = hs.regional_vp_nm THEN ss.regional_vise_president ELSE hs.regional_vp_nm END as regional_vp_nam
, CASE WHEN ss.district_manager = hs.district_mgr_nm THEN ss.district_manager ELSE hs.district_mgr_nm END as district_mgr_nam
, CASE WHEN ss.store_manager = hs.store_mgr_nm THEN ss.store_manager ELSE hs.store_mgr_nm END as store_mgr_nam
, CASE WHEN CAST(ss.tot_sq_ft AS INT64) = hs.total_area THEN ss.tot_sq_ft ELSE CAST(hs.total_area AS STRING) END as total_area_num
, ss.search_term2 as selling_des 
, CAST(hs.selling_ind AS STRING) as selling_ind
, CASE WHEN ss.sales_area_unit= CAST(hs.sales_area AS STRING) THEN ss.sales_area_unit ELSE CAST(hs.sales_area AS STRING) END as sales_area_num
, ss.sales_area_unit as sales_area_unit_cd
, CASE WHEN CAST(ss.opening_date AS INT64) = 0 THEN NULL ELSE DATE(CAST(substr(ss.opening_date,0,4) AS INT64), CAST(substr(ss.opening_date,5,2) AS INT64), CAST(substr(ss.opening_date,7) AS INT64)) END as store_open_dt
, CASE WHEN CAST(ss.closing_date as INT64) = 0 THEN NULL ELSE DATE(CAST(substr(ss.closing_date,0,4) AS INT64), CAST(substr(ss.closing_date,5,2) AS INT64), CAST(substr(ss.closing_date,7) AS INT64)) END as store_close_dt
, ss.climate as climate_des
, ss.location as location_type_cd
, ss.secondary_location as secondary_location_des
, ss.rov as rov_des
, ss.tier as tire_cd
, ss.volume as volume_cd
, ss.sales_org as sales_org_num
, ss.company_code as company_cd
, ss.distribution_channel as distribution_channel_cd
, ss.store_phone_nbr as store_phone_num
, ss.store_time_zone as store_time_zone_des
, ss.batch_id
FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_store` ss
LEFT OUTER JOIN `p-asna-analytics-002.edl_landing.ann_hst_lu_store_vw` hs ON (hs.store_nbr = CAST(store_no AS INT64))
WHERE store_no NOT LIKE 'A%' AND store_no NOT LIKE 'R%'
UNION ALL
SELECT 
store_nbr as store_key
, brand_cd
, NULL as store_status_des
, store_status_cd
, store_nm as store_nam
, store_address_1 
, store_address_2
, store_city
, store_state
, store_postal_cd
, store_country_cd
, store_latitude
, store_longitude
, region_cd
, region_desc as region_des
, district_cd
, district_desc as region_des
, tax_jurisdiction
, title_cd
, title_desc as title_des
, title_person as title_person_nam
, regional_vp_nm as regional_vp_nam
, district_mgr_nm as district_mgr_nam
, store_mgr_nm as store_mgr_nam
, CAST(total_area AS STRING) as total_area_num
, NULL AS selling_des
, CAST(selling_ind AS STRING) as selling_ind
, CAST(sales_area AS STRING) as sales_area_num
, NULL as sales_area_unit_cd
, DATE(store_open_dt) AS store_open_dt
, DATE(store_close_dt) AS store_close_dt
, climate AS climate_des
, location as location_type_cd
, secondary_location as secondary_location_des
, rov as rov_des
, tier as tire_cd
, volume as volume_cd
, sales_org_cd as sales_org_num
, company_cd
, distribution_channel_cd
, store_phone_nbr as store_phone_num
, NULL as store_time_zone_des
, batch_id
FROM `p-asna-analytics-002.edl_landing.ann_hst_lu_store_vw`
WHERE store_nbr NOT IN (SELECT CAST(store_no AS INT64) FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_store` WHERE store_no NOT LIKE 'A%' AND store_no NOT LIKE 'R%')


--PLEASE DO NOT DELETE THE BELOW CODE--
/*CREATE or REPLACE TABLE analytic_mart.pre_store
AS
SELECT store_no as store_key
, store_name as store_nam
, region as region_cd
, region_desc as region_des
, regional_vise_president as regional_vise_president_nam
, district as district_cd
, district_desc as district_des
, district_manager as district_manager_nam
, title_value as title_value_cd
, title_desc as title_des
, company_code as company_cd
, CASE WHEN CAST(opening_date AS INT64) = 0 THEN NULL ELSE DATE(CAST(substr(opening_date,0,4) AS INT64), CAST(substr(opening_date,5,2) AS INT64), CAST(substr(opening_date,7) AS INT64)) END as open_dt
, CASE WHEN CAST(closing_date as INT64) = 0 THEN NULL ELSE DATE(CAST(substr(closing_date,0,4) AS INT64), CAST(substr(closing_date,5,2) AS INT64), CAST(substr(closing_date,7) AS INT64)) END as close_dt
, secondary_location as secondary_location_cd
, store_manager as store_manager_nam
, search_term1 as search_term1_des
, search_term2 as search_term2_des
, tot_sq_ft as tot_sq_ft_num
, street as street_addr
, city as city_nam
, state as state_cd
, CASE WHEN LENGTH(zipcode) = 5 THEN zipcode WHEN LENGTH(zipcode) = 7 THEN zipcode WHEN LENGTH(zipcode)>=9 THEN substr(zipcode, 0,5) ELSE NULL END as postal_cd
, CASE WHEN LENGTH(zipcode)>=9 THEN substr(zipcode, 7) ELSE NULL END AS zip4_cd
, country as country_cd
, store_phone_nbr as store_phone_num
, location as location_type_cd
, store_time_zone as store_time_zone_des
, DATETIME(CAST(substr(date_time,0,4) AS INT64), CAST(substr(date_time,5,2) AS INT64), CAST(substr(date_time,7,2) AS INT64), CAST(substr(date_time,9,2) AS INT64), CAST(substr(date_time,11,2) AS INT64), CAST(substr(date_time,13) AS INT64)) as edl_create_tms
, 'PRE_STORE' as edl_create_job_nam
, CURRENT_DATETIME as edl_last_update_tms
, brand as brand_cd
, space_suite as space_suite_nam
, tax_jurisdiction as tax_jurisdiction_num
, sales_area as sales_area_num
, sales_area_unit as sales_area_unit_cd
, climate as climate_des
, rov as rov_des
, tier as tier_cd
, volume as volume_cd
, sales_org as  sales_org_num
, distribution_channel as distribution_channel_cd
, batch_id
from `p-asna-analytics-002.edl_landing.ann_ann_sap_store`*/