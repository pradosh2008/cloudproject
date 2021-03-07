--/*********************************************************************
--* 
--* Script Name: model score table creation and data population
--*
--**********************************************************************/
--/*

--md5(concat('JUS|',hms.model_score_run_id,'|',hms.original_individual_id)) as model_score_run_key

set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
set mapred.output.compression.type=BLOCK;

DROP TABLE ASCENA_ANALYTIC_MART_DEV.MODEL_SCORE;

CREATE TABLE IF NOT EXISTS ASCENA_ANALYTIC_MART_DEV.MODEL_SCORE(
model_score_run_key char(32)
,brand_customer_key char(32)
,model_score_run_id decimal(10,0)
,original_household_id decimal(11,0)
,original_individual_id decimal(11,0)
,scored_tms timestamp
,model_type_cd string
,division_cd string
,model_score_amt decimal(12,6)
,model_component_1_amt decimal(12,6)
,model_component_2_amt decimal(14,12)
,model_component_3_amt decimal(12,6)
,model_component_4_amt decimal(6,0)
,decile_0_num decimal(2,0)
,decile_1_num decimal(2,0)
,decile_2_num decimal(2,0)
,decile_3_num decimal(2,0)
,dependent_variable_num decimal(1,0)
,batch_id bigint
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

--temporary table creation from dedupe view ( due to vertex issue and slowness)
create temporary table ASCENA_STAGING_DEV.jus_hst_model_scores_tmp as
    select model_score_run_id
       ,original_individual_id
        ,batch_id
    from ASCENA_STAGING_DEV.jus_hst_model_scores_drc
;

create temporary table ASCENA_STAGING_DEV.jus_hst_cdi_key_xref_tmp as
    select original_individual_id
        ,current_individual_id
    from ASCENA_STAGING_DEV.jus_hst_cdi_key_xref_drc
;

create temporary table ASCENA_STAGING_DEV.jus_hst_customers_tmp as
    select individual_id
    from ASCENA_STAGING_DEV.jus_hst_customers_drc
;

create temporary table ascena_staging_dev.jus_hst_model_scores_dr_tmp as
select
 a.model_score_run_id
,a.original_household_id
,a.original_individual_id
,a.date_scored
,a.model_type_code
,a.division_code
,a.model_score
,a.model_component_value_1
,a.model_component_value_2
,a.model_component_value_3
,a.model_component_value_4
,a.decile_0
,a.decile_1
,a.decile_2
,a.decile_3
,a.dependent_variable
,a.batch_id
FROM ascena_staging_dev.jus_hst_model_scores_dr as a
inner join ASCENA_STAGING_DEV.jus_hst_model_scores_tmp as b
    on coalesce(a.model_score_run_id,'')=coalesce(b.model_score_run_id,'')
    and coalesce(a.original_individual_id,'')=coalesce(b.original_individual_id,'')
    and a.batch_id=b.batch_id;


--Insert into mart table
INSERT OVERWRITE TABLE ASCENA_ANALYTIC_MART_DEV.MODEL_SCORE
SELECT
md5(concat('JUS|',coalesce(hms.model_score_run_id,''),'|',coalesce(hms.original_individual_id,''))) as model_score_run_key
,CASE WHEN c.individual_id is not null THEN md5(concat('JUS|',coalesce(c.individual_id,''))) ELSE '-1' END as brand_customer_key
,hms.model_score_run_id as model_score_run_id
,hms.original_household_id as original_household_id
,hms.original_individual_id as original_individual_id
,from_unixtime(UNIX_TIMESTAMP(hms.date_scored,"MM/dd/yyyy HH:mm:ss")) as scored_tms
,hms.model_type_code as model_type_cd
,hms.division_code as division_cd
,hms.model_score as model_score_amt
,hms.model_component_value_1 as model_component_1_amt
,hms.model_component_value_2 as model_component_2_amt
,hms.model_component_value_3 as model_component_3_amt
,hms.model_component_value_4 as model_component_4_amt
,hms.decile_0 as decile_0_num
,hms.decile_1 as decile_1_num
,hms.decile_2 as decile_2_num
,hms.decile_3 as decile_3_num
,hms.dependent_variable as dependent_variable_num
,hms.batch_id as batch_id
FROM ascena_staging_dev.jus_hst_model_scores_dr_tmp hms
inner join ASCENA_STAGING_DEV.jus_hst_cdi_key_xref_tmp as b 
    on coalesce(hms.original_individual_id,'')=coalesce(b.original_individual_id ,'')
left outer join ascena_staging_DEV.jus_hst_customers_tmp c
    on coalesce(b.current_individual_id,'')=coalesce(c.individual_id,'');
