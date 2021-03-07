--primary key check
{code}
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
{code}
--passed

--null check
{code}
SELECT 
count(*)
FROM
work.fact_transaction_discount
where
transaction_key is null or
transaction_line_num is null or
discount_line_num is null
{code}
--passed

--Foreign Key check for transaction_key
{code}
SELECT sum(case when edw.transaction_key is null then 0 else 1 end)   as match_count
,sum(case when edw.transaction_key is null then 1 else 0 end)     as orphan_count
FROM work.fact_transaction_discount edl
LEFT OUTER JOIN `work.fact_transaction` edw 
on
edl.transaction_key =edw.transaction_key
WHERE edl.transaction_dt >= '2020-03-01' and edl.transaction_dt <='2020-03-03'
{code}
--passed 

--Foreign Key check for store_key
{code}
SELECT sum(case when edw.store_key is null then 0 else 1 end)   as match_count
,sum(case when edw.store_key is null then 1 else 0 end)     as orphan_count
FROM work.fact_transaction_discount edl
LEFT OUTER JOIN `work.dim_store` edw 
on
edl.store_key =edw.store_key
WHERE edl.transaction_dt >= '2020-03-01' and edl.transaction_dt <='2020-03-03'
{code}
--passed

--Foreign Key check for item_key
{code}
SELECT sum(case when edw.item_key is null then 0 else 1 end)   as match_count
,sum(case when edw.item_key is null then 1 else 0 end)     as orphan_count
FROM work.fact_transaction_discount edl
LEFT OUTER JOIN `work.dim_item` edw 
on
edl.item_key =edw.item_key
WHERE edl.transaction_dt >= '2020-03-01' and edl.transaction_dt <='2020-03-03'
i.division_id in ('500','510','520','454','453','455')
{code}
--failed

--Reason for the failure as provided by Sumant that many skus are not flowing through the fmw system.Investigation is still on dim_item



--comparison of transaction_key between the prod(mart built from EDL) and dev(mart built from TD)
{code}
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
{code}
--passed

--comparison of store_key between the prod and dev
{code}
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
{code}
--passed

--comparison of brand_customer_key between the prod and dev
{code}
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
{code}
--failed


--measures testing
------------------


SELECT 
 edl_dis.transaction_dt 
,sum(edl_dis.discount_amt)   as prod_discount_tot
,sum(abs(edw.discount_amt)) as dev_discount_tot
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
group by  edl_dis.transaction_dt



SELECT 
 edl_dis.transaction_dt 
,sum(edl_dis.discount_amt)   as prod_discount_tot
,sum(edw.discount_amt) as dev_discount_tot
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
     and return_ind =0
    GROUP BY
      transaction_key,
      transaction_line_num ) edl_det 
ON
edl_dis.transaction_key =edl_det.transaction_key
AND edl_dis.transaction_line_num=edl_det.transaction_line_num
LEFT OUTER JOIN `work.fact_transaction_discount` edw 
ON
edl_dis.transaction_key =edw.transaction_key
AND edl_dis.transaction_line_num= edw.transaction_line_num
AND edl_dis.discount_line_num = edw.discount_line_num
WHERE edl_dis.transaction_dt >= '2020-03-01' and edl_dis.transaction_dt <='2020-03-03'
group by  edl_dis.transaction_dt




SELECT 
 edl_dis.transaction_dt 
,sum(edl_dis.discount_amt)   as prod_discount_tot
,sum(edw.discount_amt) as dev_discount_tot
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
     and return_ind =1
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
group by  edl_dis.transaction_dt