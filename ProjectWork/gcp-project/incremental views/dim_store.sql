--/*********************************************************************
--* 
--* Script Name: BRAND_CUSTOMER_PHONE table creation and data population
--*
--**********************************************************************/

set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;

--store_key:md5(concat('JUS|',e.store_id))

drop table ascena_analytic_mart_dev.DIM_STORE ;
--Create table statement
create TABLE IF NOT EXISTS ascena_analytic_mart_dev.DIM_STORE (
     store_key           CHAR(32)
    ,brand_cd               string
    ,store_num           BIGINT
    ,start_effective_tms TIMESTAMP
    ,end_effective_tms   TIMESTAMP
    ,store_config_cd     string
    ,store_nam     string
    ,line1_addr     string
    ,line2_addr     string
    ,city_nam            string
    ,state_province_nam  string
    ,zip_postal_cd     string
    ,zip_4_cd     string
    ,country_nam string
    ,latitude_num DECIMAL(10,6)
    ,longitude_num DECIMAL(10,6)
    ,phone_num     BIGINT
    ,region_cd     string
    ,region_nam     string
    ,district_cd     string
    ,district_nam     string
    ,channel_cd  string
    ,store_status_cd    string
    ,store_type_cd      string
    ,store_location_cd     string
    ,comp_store_ind    TINYINT
    ,bts_schedule_wave_id  string          
    ,store_size_cd  string
    ,open_tms    TIMESTAMP
    ,close_tms    TIMESTAMP
    ,remodel_tms    TIMESTAMP
    ,square_footage_qty DECIMAL(10,6)	
    ,create_tms    TIMESTAMP
    ,create_job_id  BIGINT
    ,last_update_tms    TIMESTAMP
    ,last_update_job_id     string
   -- ,dw_active_ind    TINYINT
	,batch_id  BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy") 
;

--insert statement
INSERT OVERWRITE TABLE ascena_analytic_mart_dev.DIM_STORE
select md5(concat('JUS|',coalesce(e.store_id,''))) as store_key
,'JUS' as brand_cd
,e.store_no as store_num
,from_unixtime(UNIX_TIMESTAMP(e.start_effective_date,"MM/dd/yyyy HH:mm:ss")) as start_effective_tms
,from_unixtime(UNIX_TIMESTAMP(e.end_effective_date,"MM/dd/yyyy HH:mm:ss")) as end_effective_tms
,e.store_configuration as store_config_cd	-- Only 'JUSTICE' as value is there
,e.store_name as store_nam
,e.address_1 as line1_addr
,e.address_2 as line2_addr
,e.city as city_nam
,e.state_province as state_province_nam
,e.zip_postal_code as zip_postal_cd
,e.zip_4 as zip_4_cd
,e.country as country_nam
,e.latitude as latitude_num
,e.longitude as longitude_num
,e.phone_no as phone_num
,e.region_code as region_cd
,e.region_name as region_nam
,e.district_code as district_cd
,e.district_name as district_nam
,e.channel_code as channel_cd      -- Has C S W value
,e.store_status as store_status_cd -- OPEN OR CLOSED value
,e.store_type as store_type_cd
,e.store_location_code as store_location_cd
,CASE WHEN e.comp_store_flag ='Y' THEN 1 WHEN e.comp_store_flag ='N' THEN 0  ELSE 0 END as comp_store_ind 
,e.bts_schedule_wave as bts_schedule_wave_id  -- values 0 1 2 .... 7 NULL are there
,e.store_size_code as store_size_cd      -- Only NULL is there 
,from_unixtime(UNIX_TIMESTAMP(e.open_date,"MM/dd/yyyy HH:mm:ss")) as open_tms
,from_unixtime(UNIX_TIMESTAMP(e.close_date,"MM/dd/yyyy HH:mm:ss")) as close_tms
,from_unixtime(UNIX_TIMESTAMP(e.remodel_date,"MM/dd/yyyy HH:mm:ss")) as remodel_tms
,e.square_footage as square_footage_qty
,from_unixtime(UNIX_TIMESTAMP(e.create_date,"MM/dd/yyyy HH:mm:ss")) as create_tms
,e.create_job_id as create_job_id
,from_unixtime(UNIX_TIMESTAMP(e.last_update_date,"MM/dd/yyyy HH:mm:ss")) as last_update_tms
,e.last_update_job_id as last_update_job_id
,e.batch_id as batch_id
from ASCENA_STAGING_DEV.jus_hst_stores_drc as e;

--Primary Key check
--select store_key from ascena_analytic_mart_dev.DIM_STORE group by store_key having count(*)>1;


