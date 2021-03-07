
--/*********************************************************************
--
-- Script Name: PROMOTION table creation and data population
--
--**********************************************************************/

-- division_code :	BRO,JUS
-- business_unit_code : TWE
-- promotion_type : AD-HOC,BIRTHDAY,CAT-PRIVATE SALE,CATAZINE,CATAZINE-POS,CLEARANCE,DEEP PURPLE,FLASH SALE,FUN CARD,JBUCKS,OTHER,PC-PRIVATE SALE,POSTCARD,POSTCARD-POS,TRIGGER-AB CART,TRIGGER-LOYALTY,TRIGGER-PROSPECT,TRIGGER-THANK YOU,TRIGGER-WELCOME
-- promotion_channel : BOUNCEBACK,DM, EM , SMS
-- PROMOTION_SEASON  : Fall,Spring,Summer,Unknown,Winter
-- PROMOTION_STATUS : Active Cancelled Completed Historical Not Started
-- promotion_cnt_as_contact_flag : Y N
-- FORCE_RESPONSE_ATTRIBUTION_FLG : N


set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


--PROMOTION_KEY: md5(concat('JUS|',e.promotion_id))


CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.DIM_PROMOTION_may(
PROMOTION_KEY	CHAR(32),
PROGRAM_KEY CHAR(32),
PROMOTION_ID BIGINT,
CAMPAIGN_CD	STRING,
PROMOTION_KEY_CD	STRING,
PROGRAM_ID	BIGINT,
DIVISION_CD	STRING,
BUSINESS_UNIT_CD	STRING,
PROMOTION_TYPE_CD	STRING,
PROMOTION_CHANNEL_CD	STRING,
PROMOTION_NM	STRING,
PROMOTION_DES	STRING,
PROMOTION_SEASON_CD	STRING,
PROMOTION_STATUS_CD	STRING,
COUNTRY_CD	STRING,
PROMOTION_START_DT	DATE,
PROMOTION_END_DT	DATE,
PROMOTION_OBJECTIVE_DES	STRING,
ALLANT_MANAGER_NM	STRING,
ASCENA_MARKETING_MANAGER_NM	STRING,
PROMOTION_CNT_AS_CONTACT_IND	TINYINT,
CREATE_TMS	TIMESTAMP,
CREATE_USER_ID	STRING,
LAST_UPDATE_TMS	TIMESTAMP,
LAST_UPDATE_USER_ID	STRING,
FORCE_RESPONSE_ATTRIBUTION_IND	TINYINT,
INHOME_DT	DATE,
TRIGGER_RESPONSE_DAYS_NUM	BIGINT,
BATCH_ID	BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


INSERT OVERWRITE TABLE ascena_analytic_mart_dev.dim_promotion_may
select 
md5(concat('JUS|',coalesce(a.promotion_id,''))) as PROMOTION_KEY
, md5(concat('JUS|',coalesce(a.program_id,''))) as PROGRAM_KEY
,a.promotion_id as PROMOTION_ID
,a.campaign_code as CAMPAIGN_CD
,a.promotion_key_code as PROMOTION_KEY_CD
,a.program_id as PROGRAM_ID
,a.division_code as DIVISION_CD
,a.business_unit_code as BUSINESS_UNIT_CD
,a.promotion_type as PROMOTION_TYPE_CD
,a.promotion_channel as PROMOTION_CHANNEL_CD
,a.promotion_name as PROMOTION_NM
,a.promotion_description as PROMOTION_DES
,a.promotion_season as PROMOTION_SEASON_CD
,a.promotion_status as PROMOTION_STATUS_CD
,a.country as COUNTRY_CD
,to_date(from_unixtime(UNIX_TIMESTAMP(a.promotion_start_date,"MM/dd/yyyy"))) as	PROMOTION_START_DT
,to_date(from_unixtime(UNIX_TIMESTAMP(a.promotion_end_date,"MM/dd/yyyy"))) as PROMOTION_END_DT
,a.promotion_objective as PROMOTION_OBJECTIVE_DES
,a.allant_manager as ALLANT_MANAGER_NM
,a.ascena_marketing_manager as ASCENA_MARKETING_MANAGER_NM
,CASE a.promotion_cnt_as_contact_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as	PROMOTION_CNT_AS_CONTACT_IND
,from_unixtime(UNIX_TIMESTAMP(a.create_date,"MM/dd/yyyy HH:mm:ss")) as CREATE_TMS
,a.create_user_id as CREATE_USER_ID
,from_unixtime(UNIX_TIMESTAMP(a.last_update_date,"MM/dd/yyyy HH:mm:ss")) as	LAST_UPDATE_TMS
,a.last_update_user_id as LAST_UPDATE_USER_ID
,CASE a.force_response_attribution_flg WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as FORCE_RESPONSE_ATTRIBUTION_IND
,to_date(from_unixtime(UNIX_TIMESTAMP(a.inhome_date,"MM/dd/yyyy"))) as INHOME_DT
,a.trigger_response_days as TRIGGER_RESPONSE_DAYS_NUM
,a.batch_id as BATCH_ID from
ascena_staging_dev.jus_hst_promotions_dr as a where a.batch_id='20190501';

