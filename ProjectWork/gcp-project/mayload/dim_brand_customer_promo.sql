--/*********************************************************************
--* 
--* Script Name: dim_brand_customer_promo table creation and data population
--*
--**********************************************************************/


set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;

drop table ascena_analytic_mart_dev.dim_brand_customer_promo ;

CREATE table IF NOT EXISTS ascena_analytic_mart_dev.dim_brand_customer_promo (
brand_customer_key     char(32)
,brand_cd               string
,individual_id bigint
,batch_id BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

-- BRAND_CUSTOMER_KEY is the primary key
INSERT OVERWRITE TABLE ascena_analytic_mart_dev.dim_brand_customer_promo
select 
md5(concat('JUS|',coalesce(xref.current_individual_id,''))) as brand_customer_key 
 ,'JUS' as brand_cd
,xref.current_individual_id  AS individual_id
,20170101 as batch_id
from ascena_staging_dev.jus_dr_promotion_history_drh prom
join ascena_staging_DEV.jus_hst_cdi_key_xref_drc xref 
    on prom.original_individual_id=xref.original_individual_id 
left join ascena_staging_DEV.jus_hst_customers_drc cust
on xref.current_individual_id=cust.individual_id
where cust.individual_id is null
group by xref.current_individual_id
;

