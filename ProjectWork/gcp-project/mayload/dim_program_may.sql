--/*********************************************************************
--
-- Script Name: PROGRAM table creation and data population
--
--**********************************************************************/

-- PROGRAM_TYPE : BIRTHDAY BOUNCEBACK CATAZINE LEGACY DATA MERCHANDISE OTHER POSTCARD TRIGGER WELCOME
-- PROGRAM_STATUS : Active Closed


set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;

-- md5(concat('JUS|',b.program_id))	as	PROGRAM_KEY
CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.DIM_PROGRAM_MAY(
PROGRAM_KEY	CHAR(32),
PROGRAM_ID BIGINT,
PROGRAM_TYPE_CD	STRING,
BUSINESS_UNIT_CD STRING,
PROGRAM_CD	STRING,
PROGRAM_NM	STRING,
PROGRAM_DES	STRING,
PROGRAM_STATUS_CD	STRING,
LY_PROGRAM_CD	STRING,
LAST_UPDATE_USER_ID	STRING,
LAST_UPDATE_TMS	TIMESTAMP,
BATCH_ID	BIGINT)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


INSERT OVERWRITE TABLE ascena_analytic_mart_dev.dim_program_may
select 
 md5(concat('JUS|',coalesce(b.program_id,''))) as PROGRAM_KEY
,b.program_id as PROGRAM_ID
,b.program_type as PROGRAM_TYPE_CD
,b.business_unit_code as BUSINESS_UNIT_CD
,b.program_code as PROGRAM_CD
,b.program_name as PROGRAM_NM
,b.program_description as PROGRAM_DES
,b.program_status as PROGRAM_STATUS_CD
,b.ly_program_code as LY_PROGRAM_CD
,b.last_update_user_id as LAST_UPDATE_USER_ID
,from_unixtime(UNIX_TIMESTAMP(b.last_update_date,"MM/dd/yyyy HH:mm:ss")) as LAST_UPDATE_TMS
,b.batch_id as BATCH_ID 
from ascena_staging_dev.jus_hst_programs_dr as b where b.batch_id='20190501';
