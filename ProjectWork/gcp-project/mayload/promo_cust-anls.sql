--/*********************************************************************
--* 
--* Script Name: dim_brand_customer_promo table creation and data population
--*
--**********************************************************************/

--drop table ascena_analytic_mart_dev.wrk_brand_customer_promo ;

CREATE table IF NOT EXISTS ascena_analytic_mart_dev.wrk_brand_customer_promo 
(    brand_customer_key     char(32)
    ,brand_cd               string
    ,individual_id          bigint
    ,batch_id               bigint
)
STORED AS ORC;  

create temporary table jus_hst_customers_tmp 
as select *
from ascena_staging_DEV.jus_hst_customers_drc cust
;

-- BRAND_CUSTOMER_KEY is the primary key
INSERT OVERWRITE TABLE ascena_analytic_mart_dev.wrk_brand_customer_promo
select md5(concat('JUS|',coalesce(xref.current_individual_id,''))) as brand_customer_key 
    ,'JUS' as brand_cd
    ,xref.current_individual_id  AS individual_id
    ,20170101 as batch_id
from (select x.original_individual_id 
            , x.current_individual_id
    from ascena_staging_DEV.jus_hst_cdi_key_xref_drc x               --2770273855
    left join jus_hst_customers_tmp cust
        on x.current_individual_id=cust.individual_id
    where cust.individual_id is null
) xref
join ascena_staging_dev.jus_dr_promotion_history_drh prom           --1554912331
    on prom.original_individual_id=xref.original_individual_id 
group by xref.current_individual_id
,md5(concat('JUS|',coalesce(xref.current_individual_id,'')))
;

