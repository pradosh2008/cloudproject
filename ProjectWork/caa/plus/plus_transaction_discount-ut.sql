#primary key check
bq query --use_legacy_sql=false <<!
SELECT 
transaction_key,
transaction_line_num,
discount_line_num
FROM
work.fact_transaction_discount
GROUP BY 
transaction_key,
transaction_line_num,
discount_line_num
HAVING COUNT(*)>1
limit 5
!

#null check
bq query --use_legacy_sql=false <<!
SELECT 
count(*)
FROM
work.fact_transaction_discount
where
transaction_key is null or
transaction_line_num is null or
discount_line_num is null
!

#Foreign Key check for transaction_key
bq query --use_legacy_sql=false <<!
SELECT sum(case when edw.transaction_key is null then 0 else 1 end)   as match_count
,sum(case when edw.transaction_key is null then 1 else 0 end)     as orphan_count
FROM work.fact_transaction_discount edl
LEFT OUTER JOIN `work.fact_transaction` edw 
on
edl.transaction_key =edw.transaction_key
WHERE edl.transaction_dt >= '2020-03-01' and edl.transaction_dt <='2020-03-03'
!
--passed 

#Foreign Key check for store_key
SELECT sum(case when edw.store_key is null then 0 else 1 end)   as match_count
,sum(case when edw.store_key is null then 1 else 0 end)     as orphan_count
FROM work.fact_transaction_discount edl
LEFT OUTER JOIN `work.dim_store` edw 
on
edl.store_key =edw.store_key
WHERE edl.transaction_dt >= '2020-03-01' and edl.transaction_dt <='2020-03-03'

--passed

SELECT sum(case when edw.item_key is null then 0 else 1 end)   as match_count
,sum(case when edw.item_key is null then 1 else 0 end)     as orphan_count
FROM work.fact_transaction_discount edl
LEFT OUTER JOIN `work.dim_item` edw 
on
edl.item_key =edw.item_key
WHERE edl.transaction_dt >= '2020-03-01' and edl.transaction_dt <='2020-03-03'

--failed

--comparison of transaction_key between the prod and dev

SELECT 
 sum(case when edw.transaction_key is null then 0 else 1 end)   as match_count
,sum(case when edw.transaction_key is null then 1 else 0 end)     as orphan_count
FROM `p-asna-analytics-002.analytic_mart.fact_transaction_discount` edl_dis
INNER JOIN (
    SELECT
      transaction_key,
      transaction_line_num
    FROM
      `p-asna-analytics-002.analytic_mart.fact_transaction_detail`
    WHERE
     line_object_cd !='110'
    GROUP BY
      transaction_key,
      transaction_line_num ) edl_det 
on
edl_dis.transaction_key =edl_det.transaction_key
and edl_dis.transaction_line_num=edl_det.transaction_line_num
LEFT OUTER JOIN `work.fact_transaction_discount` edw 
on
edl_dis.transaction_key =edw.transaction_key
and edl_dis.transaction_line_num= edw.transaction_line_num
and edl_dis.discount_line_num = edw.discount_line_num
WHERE edl_dis.transaction_dt >= '2020-03-01' and edl_dis.transaction_dt <='2020-03-03'

--passed

--comparison of store_key between the prod and dev

SELECT 
sum(case when edw.store_key=edl_dis.store_key then 1 else 0 end)   as match_count
,sum(case when edw.store_key=edl_dis.store_key  then 0 else 1 end)     as orphan_count
FROM `p-asna-analytics-002.analytic_mart.fact_transaction_discount` edl_dis
INNER JOIN (
    SELECT
      transaction_key,
      transaction_line_num
    FROM
      `p-asna-analytics-002.analytic_mart.fact_transaction_detail`
    WHERE
      --transaction_dt = '2020-02-09' and 
     line_object_cd !='110'
    GROUP BY
      transaction_key,
      transaction_line_num ) edl_det 
on
edl_dis.transaction_key =edl_det.transaction_key
and edl_dis.transaction_line_num=edl_det.transaction_line_num
LEFT OUTER JOIN `work.fact_transaction_discount` edw 
on
edl_dis.transaction_key =edw.transaction_key
and edl_dis.transaction_line_num= edw.transaction_line_num
and edl_dis.discount_line_num = edw.discount_line_num
WHERE edl_dis.transaction_dt >= '2020-03-01' and edl_dis.transaction_dt <='2020-03-03'

--passed

--comparison of brand_customer_key between the prod and dev

SELECT 
sum(case when edw.brand_customer_key = edl_dis.brand_customer_key then 1 else 0 end)   as match_count
,sum(case when edw.brand_customer_key = edl_dis.brand_customer_key  then 0 else 1 end)     as orphan_count
FROM `p-asna-analytics-002.analytic_mart.fact_transaction_discount` edl_dis
INNER JOIN (
    SELECT
      transaction_key,
      transaction_line_num
    FROM
      `p-asna-analytics-002.analytic_mart.fact_transaction_detail`
    WHERE
      --transaction_dt = '2020-02-09' and 
     line_object_cd !='110'
    GROUP BY
      transaction_key,
      transaction_line_num ) edl_det 
on
edl_dis.transaction_key =edl_det.transaction_key
and edl_dis.transaction_line_num=edl_det.transaction_line_num
LEFT OUTER JOIN `work.fact_transaction_discount` edw 
on
edl_dis.transaction_key =edw.transaction_key
and edl_dis.transaction_line_num= edw.transaction_line_num
and edl_dis.discount_line_num = edw.discount_line_num
WHERE edl_dis.transaction_dt >= '2020-03-01' and edl_dis.transaction_dt <='2020-03-03'