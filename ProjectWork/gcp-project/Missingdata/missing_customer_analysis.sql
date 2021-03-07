
--creating dedup from drh table
CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_customers_drh_dedup
AS
SELECT TBL.*
from ( SELECT d.*
             , row_number() over ( partition by individual_id
                                   order by BATCH_ID desc
                          ) as rn
       from ascena_staging_dev.jus_hst_customers_drh d
     ) as TBL
where rn=1
;

drop table work_dim_brand_customer;
create table if not exists ascena_analytic_mart_dev.work_dim_brand_customer
(
individual_id  int
,brand_customer_key  CHAR(32)
)
stored as orc;

--consolidated records from drh_dedup and dim_brand_customer
insert into ascena_analytic_mart_dev.work_dim_brand_customer 
select * from (
select
    individual_id
    ,md5(concat('JUS|',coalesce(individual_id,''))) as brand_customer_key
from 
ASCENA_STAGING_DEV.jus_hst_customers_drh_dedup 
union
select
    individual_id
    ,brand_customer_key
from 
ascena_analytic_mart_dev.dim_brand_customer) as e
group by individual_id,brand_customer_key;

drop table ascena_staging_dev.orphan_work_brand_customer;
CREATE TABLE if not exists ascena_staging_dev.orphan_work_brand_customer
(
brand_customer_key CHAR(32)
)
stored as orc;

--Loading the missing records

INSERT into ascena_staging_dev.orphan_work_brand_customer
select  p.brand_customer_key 
from    ascena_analytic_mart_dev.promo_cust_reference p 
 left outer join ascena_analytic_mart_dev.work_dim_brand_customer c
  on p.brand_customer_key=c.brand_customer_key
  where c.brand_customer_key is null;


--distinct count of missing brand customer key
select  count(*) 
from    (
        select  brand_customer_key,count(*) 
        from    ascena_staging_dev.orphan_work_brand_customer 
        group by brand_customer_key) as a;


drop table ascena_staging_dev.orphan_baseline_brand_customer2;
create table ascena_staging_dev.orphan_baseline_brand_customer2
stored as orc
as
select distinct brand_customer_key
from ascena_staging_dev.orphan_baseline_brand_customer
;

-- The previously unmatched and now matching count
select count(o.brand_customer_key) as orphan_cnt
    ,count(distinct w.brand_customer_key)  as found_cnt
from ascena_staging_dev.orphan_baseline_brand_customer2 o
left join ascena_analytic_mart_dev.work_dim_brand_customer w
    on w.brand_customer_key = o.brand_customer_key
;
