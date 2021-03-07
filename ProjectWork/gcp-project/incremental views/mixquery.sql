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

select model_component_value_1 ,model_component_value_2 from ascena_staging_dev.jus_hst_model_scores_20190105 where model_score_run_id=583 and original_individual_id=9081523185




select
count(*)
from ascena_staging_dev.jus_hst_promotion_history_20190105 as a
inner join ascena_staging_DEV.jus_hst_cdi_key_xref_20190104 as b 
    on a.original_individual_id=b.original_individual_id 
left outer join ascena_staging_DEV.jus_hst_customers_20190104 c
    on b.current_individual_id=c.individual_id
;

--577490668
+-----------------------------------+------+--+
|     promo_cust_reference_key      | _c1  |
+-----------------------------------+------+--+
| 006f79d45400cac5e3c8dcafdcf10919  | 2    |
| 021cbde01b3ba1fee131018276bc5c86  | 2    |
| 05cf3d11d9439299477fba987966dd94  | 2    |
| 0631c3cf92362006d22df3eaf8c287d6  | 2    |
| 07f17bed15b3111de7676ad357b5abf1  | 2    |
+-----------------------------------+------+--+
select
a.promotion_id,
a.promoted_entity_id,
coalesce(from_unixtime(UNIX_TIMESTAMP(a.promotion_execution_date,"MM/dd/yyyy HH:mm:ss"),"yyyyMMdd HH:mm:ss"),'')
from ascena_staging_dev.jus_hst_promotion_history_20190105 as a where
md5(concat('JUS|',coalesce(a.promotion_id,''),'|',coalesce(a.promoted_entity_id,''),'|',coalesce(from_unixtime(UNIX_TIMESTAMP(a.promotion_execution_date,"MM/dd/yyyy"), "yyyyMMdd"),'')))='006f79d45400cac5e3c8dcafdcf10919'

+-----------------+-----------------------+-----------------------------+--+
| a.promotion_id  | a.promoted_entity_id  | a.promotion_execution_date  |
+-----------------+-----------------------+-----------------------------+--+
| 1105683         | 310151741             | 11/20/2018 02:55:03         |
| 1105683         | 310151741             | 11/20/2018 17:15:02         |
+-----------------+-----------------------+-----------------------------+--+


+-----------------+-----------------------+--------------------+--+
| a.promotion_id  | a.promoted_entity_id  |        _c2         |
+-----------------+-----------------------+--------------------+--+
| 1105683         | 310151741             | 20181120 02:55:03  |
| 1105683         | 310151741             | 20181120 17:15:02  |
+-----------------+-----------------------+--------------------+--+

jus_hst_lists_drc
