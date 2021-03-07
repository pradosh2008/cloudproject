--/*********************************************************************
--* 
--* Script Name: DIM_PROMO_EXECUTION_HIERARCHY table creation and data population
--*
--**********************************************************************/
--/*
--md5(concat('JUS', s.promotion_id, s.segment_id, trim(coalesce(c.cell_code, '')), s.promotion_execution_id))

set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.DIM_PROMO_EXECUTION_HIERARCHY_MAY (
PROMO_EXEC_HIER_KEY CHAR(32)
, promotion_key CHAR(32)
, segment_id BIGINT
, cell_cd STRING
, promotion_execution_id BIGINT
, package_id BIGINT
, panel_id INT
, segment_des STRING
, cell_des STRING
, segment_grp_1_des STRING
, segment_grp_2_des STRING
, segment_grp_3_des STRING
, selection_type_cd STRING
, hard_start_dt DATE
, hard_end_dt DATE
, inhome_dt DATE
, last_update_dt TIMESTAMP
, batch_id BIGINT
) 
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


INSERT OVERWRITE TABLE ascena_analytic_mart_dev.dim_promo_execution_hierarchy_may
SELECT md5(concat('JUS|', coalesce(s.promotion_id, ''), '|', coalesce(s.segment_id, ''), '|', coalesce(upper(trim(c.cell_code)), ''), '|', coalesce(s.promotion_execution_id, ''))) as PROMO_EXEC_HIER_KEY
, md5(concat('JUS|', coalesce(s.promotion_id, ''))) as PROMOTION_KEY
, s.segment_id as segment_id
, c.cell_code as cell_cd
, s.promotion_execution_id as promotion_execution_id
, c.package_id
, c.panel_id as panel_id
, s.segment_description as segment_des
, c.cell_description as cell_des
, s.segment_group_1 as segment_grp_1_des
, s.segment_group_2 as segment_grp_2_des
, s.segment_group_3 as segment_grp_3_des
, c.selection_type_code as selection_type_cd
, from_unixtime(UNIX_TIMESTAMP(c.hard_start_date, "MM/dd/yyyy HH:mm:ss")) as hard_start_dt
, from_unixtime(UNIX_TIMESTAMP(c.hard_end_date, "MM/dd/yyyy HH:mm:ss")) as hard_end_dt
, from_unixtime(UNIX_TIMESTAMP(c.inhome_date, "MM/dd/yyyy HH:mm:ss")) as inhome_dt
, from_unixtime(UNIX_TIMESTAMP(s.last_update_date,"MM/dd/yyyy HH:mm:ss")) as last_update_dt
, s.batch_id as batch_id
FROM ascena_staging_dev.jus_hst_promotion_segments_dr as s
LEFT OUTER JOIN ascena_staging_dev.jus_hst_promotion_cells_dr as c
ON s.segment_id = c.segment_id 
AND s.promotion_id = c.promotion_id 
AND s.promotion_execution_id = c.promotion_execution_id
where s.batch_id='20190501' and c.batch_id='20190501';
