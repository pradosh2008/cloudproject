--/*********************************************************************
--* 
--* Script Name: DIM_PACKAGE_TREATMENT table creation and data population
--*
--**********************************************************************/
--/*
--md5(concat('JUS|', coalesce(t.package_id, ''), '|', coalesce(t.treatment_code, '')))

set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;
!set force true;

drop TABLE ascena_analytic_mart_dev.dim_package_treatment;

CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.dim_package_treatment (
PACKAGE_TREATMENT_KEY CHAR(32)
, TREATMENT_KEY CHAR(32)
, PACKAGE_ID BIGINT
, TREATMENT_CD STRING
, PACKAGE_DES STRING
, COST_AMT FLOAT
, CREATIVE_COST_AMT FLOAT
, POSTAGE_COST_AMT FLOAT
, PRODUCTION_COST_AMT FLOAT
, OTHER_COST_AMT FLOAT
, LANGUAGE_DES STRING
, LAST_UPDATE_DT TIMESTAMP
, LAST_UPDATE_USER_ID STRING
, BATCH_ID BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

INSERT OVERWRITE TABLE ascena_analytic_mart_dev.DIM_PACKAGE_TREATMENT
SELECT md5(concat('JUS|', coalesce(t.package_id, ''), '|', coalesce(upper(trim(t.treatment_code)), ''))) as PACKAGE_TREATMENT_KEY
,md5(concat('JUS|', coalesce(upper(trim(t.treatment_code)), ''))) as TREATMENT_KEY
, p.package_id as package_id
, t.treatment_code as treatment_cd
, p.package_description as package_des
, coalesce(p.cost, CAST(0 AS FLOAT)) as cost_amt
, coalesce(p.creative_cost, CAST(0 AS FLOAT)) as creative_cost_amt
, coalesce(p.postage_cost, CAST(0 AS FLOAT)) as postage_cost_amt
, coalesce(p.production_cost, CAST(0 AS FLOAT)) as production_cost_amt
, coalesce(p.other_cost, CAST(0 AS FLOAT)) as other_cost_amt
, p.language as language_des
, from_unixtime(UNIX_TIMESTAMP(p.last_update_date,"MM/dd/yyyy HH:mm:ss")) as last_update_dt
, p.last_update_user_id as last_update_user_id
, p.batch_id as batch_id
FROM ascena_staging_dev.jus_hst_package_treatments_drc as t
LEFT OUTER JOIN ascena_staging_dev.jus_hst_packages_drc as p
ON p.package_id = t.package_id;
