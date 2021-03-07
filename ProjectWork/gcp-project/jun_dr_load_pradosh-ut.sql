!record jun_dr_load-ut.txt
select case when sum(c1)-sum(c2)==0 then 'promotions-matched counts' else 'promotions-mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_staging_dev.jus_hst_promotions_dr where batch_id='20190601'
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart_dev.dim_promotion_jun
) as a
;


select case when sum(c1)-sum(c2)==0 then 'promo_execution_hierarchy-matched counts' else 'promo_execution_hierarchy-mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 FROM ascena_staging_dev.jus_hst_promotion_segments_dr as s
LEFT OUTER JOIN ascena_staging_dev.jus_hst_promotion_cells_dr as c
ON s.segment_id = c.segment_id
AND s.promotion_id = c.promotion_id
AND s.promotion_execution_id = c.promotion_execution_id
where s.batch_id='20190601' and c.batch_id='20190601'
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart_dev.dim_promo_execution_hierarchy_jun
) as a
;


select case when sum(c1)-sum(c2)==0 then 'package_treatment-matched counts' else 'package_treatment-mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 FROM ascena_staging_dev.jus_hst_package_treatments_dr as t
LEFT OUTER JOIN ascena_staging_dev.jus_hst_packages_dr as p
ON p.package_id = t.package_id
where p.batch_id='20190601' and t.batch_id='20190601'
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart_dev.dim_package_treatment_jun
) as a
;

select case when sum(c1)-sum(c2)==0 then 'customer_email-matched counts' else 'customer_email-mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from (select
row_number() over(partition by e.individual_id, e.email_address
                           order by from_unixtime(UNIX_TIMESTAMP(e.last_update_time,"MM/dd/yyyy HH:mm:ss")) desc
                        )                                                               as rn
    from ASCENA_STAGING_DEV.jus_hst_customer_email_addresses_dr as e where e.batch_id='20190601'
) as m
where rn=1
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart_dev.dim_brand_customer_email_jun
) as a
;

select case when sum(c1)-sum(c2)==0 then 'customer_phones-matched counts' else 'customer_phones-mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ASCENA_STAGING_DEV.jus_hst_customer_phones_dr where batch_id='20190601'
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart_dev.dim_brand_customer_phone_jun
) as a
;

select case when sum(c1)-sum(c2)==0 then 'brand_customer-matched counts' else 'brand_customer-mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ASCENA_STAGING_DEV.jus_hst_customers_dr where batch_id='20190601'
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart_dev.dim_brand_customer_jun
) as a
;

select case when sum(c1)-sum(c2)==0 then 'store-matched counts' else 'store-mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_staging_dev.jus_hst_stores_dr where batch_id='20190601'
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart_dev.dim_store
) as a
;