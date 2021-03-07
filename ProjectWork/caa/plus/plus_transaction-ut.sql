#primary key check
bq query --use_legacy_sql=false <<!
select a.transaction_key 
    ,count(*)
from work.fact_transaction a
group by a.transaction_key 
having count(*) > 1
limit 10
!

#passed

#null check
bq query --use_legacy_sql=false <<!
SELECT count(*) 
FROM    work.fact_transaction
where   transaction_key is null
!

#passed

#Comparison of transaction count for a single day for LB
bq query --use_legacy_sql=false <<!
SELECT sum(case when edw.transaction_key is null then 0 else 1 end)   as match_count
,sum(case when edw.transaction_key is null then 1 else 0 end)     as orphan_count
FROM `p-asna-analytics-002.analytic_mart.fact_transaction` edl
LEFT OUTER JOIN `work.fact_transaction` edw 
on
edl.transaction_key =edw.transaction_key 
WHERE edl.transaction_dt > '2020-02-12' and edl.transaction_dt < '2020-02-22'
!

#passed

#Comparison of brand_customer_key 
bq query --use_legacy_sql=false <<!
SELECT 
sum(case when edw.brand_customer_key=edl.brand_customer_key then 1 else 0 end)   as match_count
,sum(case when edw.brand_customer_key=edl.brand_customer_key  then 0 else 1 end)     as orphan_count
FROM `p-asna-analytics-002.analytic_mart.fact_transaction` edl
LEFT OUTER JOIN `work.fact_transaction` edw 
on
edl.transaction_key =edw.transaction_key 
WHERE edl.transaction_dt > '2020-02-12' and edl.transaction_dt < '2020-02-22'
--and edl.brand_customer_key<>edw.brand_customer_key
!

#failed

#Comparison of store key
bq query --use_legacy_sql=false <<!
SELECT 
sum(case when edw.store_key=edl.store_key then 1 else 0 end)   as match_count
,sum(case when edw.store_key=edl.store_key  then 0 else 1 end)     as orphan_count
FROM `p-asna-analytics-002.analytic_mart.fact_transaction` edl
LEFT OUTER JOIN `work.fact_transaction` edw 
on
edl.transaction_key =edw.transaction_key 
WHERE edl.transaction_dt > '2020-02-12' and edl.transaction_dt < '2020-02-22'
!

#passed