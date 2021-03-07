--/*********************************************************************
--* 
--* Script Name: dim_brand_customer_promo table creation and data population
--*
--**********************************************************************/


set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE table IF NOT EXISTS ascena_analytic_mart_dev.dim_brand_customer_promo_may (
brand_customer_key     char(32)
,brand_cd               string
,individual_id bigint
,batch_id BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


INSERT OVERWRITE TABLE ascena_analytic_mart_dev.dim_brand_customer_promo_may
select md5(concat('JUS|',coalesce(xref.current_individual_id,''))) as brand_customer_key 
    ,'JUS' as brand_cd
    ,xref.current_individual_id  AS individual_id
    ,20190501 as batch_id
from (select x.original_individual_id 
            , x.current_individual_id
    from ascena_staging_DEV.jus_hst_cdi_key_xref_dr x  
    left join ascena_staging_DEV.jus_hst_customers_dr cust
        on x.current_individual_id=cust.individual_id
    where cust.individual_id is null and x.batch_id='20190501'
) xref
join ascena_staging_dev.jus_hst_promotion_history_dr prom
    on prom.original_individual_id=xref.original_individual_id
group by xref.current_individual_id
,md5(concat('JUS|',coalesce(xref.current_individual_id,'')))
;