----------transaction test cases-------------------
----------------------------------------------------

--number of distinct transaction in teradata for LB
select count(*) from (
SELECT transaction_nbr,
    selling_store_nbr,
    selling_chain_nbr,
    register_nbr,
    transaction_dt
	,count(*) as cnt
	FROM EDWP_LB_RPT.SALES_TRANSACTION_header 
	WHERE  selling_chain_nbr=7 and transaction_dt > '2020-02-12' and  transaction_dt < '2020-02-22' 
	group by transaction_nbr ,
    selling_store_nbr ,
    selling_chain_nbr ,
    register_nbr ,
    transaction_dt
	) AS a;
	

--392771

--number of distinct transaction in GCP for LB
select count(*) from work.fact_transaction 
where brand_cd ='LB' 
and transaction_dt > '2020-02-12' and transaction_dt < '2020-02-22'

--392771

--number of distinct transaction in teradata for CA
select count(*) from (
SELECT transaction_nbr,
    selling_store_nbr,
    selling_chain_nbr,
    register_nbr,
    transaction_dt
	,count(*) as cnt
	FROM EDWP_CA_RPT.SALES_TRANSACTION_header 
	WHERE  selling_chain_nbr=4 and transaction_dt > '2020-02-12' and  transaction_dt < '2020-02-22' 
	group by transaction_nbr ,
    selling_store_nbr ,
    selling_chain_nbr ,
    register_nbr ,
    transaction_dt
	) AS a;
	
--103311	

--number of distinct transaction in GCP for CA
select count(*) from work.fact_transaction 
where brand_cd ='CA' 
and transaction_dt > '2020-02-12' and transaction_dt < '2020-02-22' 

--103311	
	
	
-- a sample transaction in teradata
SELECT * FROM EDWP_LB_RPT.SALES_TRANSACTION_header WHERE transaction_nbr = 9025  and selling_store_nbr=4267 and selling_chain_nbr=7 and register_nbr=1 and transaction_dt = '2020-02-12'

--ship_to_store_nbr is 0 for ecom transactions  (in gcp)
select 	selling_store_nbr from `edl_stage.plus_sales_transaction_header` where ship_to_store_nbr=0   group by selling_store_nbr

--analysis on customer nbr 0 or null
select * from `edl_stage.plus_sales_transaction_header` where customer_nbr is null or customer_nbr =0
select * from `work.transaction` where brand_customer_key is null or brand_customer_key='8d94d2b434736a1ded806fe96911519f' 


select * from `edl_stage.plus_sales_transaction_header` where TO_HEX(MD5(concat(CAST(case when selling_chain_nbr = 7 then 'LB' else 'CA' end as string)
    ,'|',CAST (selling_store_nbr AS string)
    ,'|',CAST (register_nbr AS string)
    ,'|',CAST(transaction_dt as string)
    ,'|',CAST(transaction_nbr AS string))))='f7de00b9e12ff29d457d8147a95f3557'
	
	

--select * from `edl_stage.plus_sales_transaction_header`  where transaction_dt='2020-02-13' and transaction_nbr=6377 and 
-- TO_HEX(MD5(concat(CAST(case when selling_chain_nbr = 7 then 'LB' else 'CA' end as string),'|',CAST(selling_store_nbr AS string))))='48fa31450b22c0a0b3988946877ea4e5' and register_nbr=1 



--customer_nbr is null or customer_nbr =0 then brand_customer_key is 0

--check the transaction which are there EDL  but not in teradata GCP

-- SELECT sum(case when edw.transaction_key is null then 0 else 1 end)   as match_count
-- ,sum(case when edw.transaction_key is null then 1 else 0 end)     as orphan_count
-- FROM `p-asna-analytics-002.analytic_mart.fact_transaction` edl
-- LEFT OUTER JOIN `work.transaction` edw 
-- on
-- edl.transaction_key =edw.transaction_key 
-- WHERE edl.transaction_dt > '2020-02-12' and edl.transaction_dt < '2020-02-22'
--and edl.brand_cd ='CA'


--check those transaction which are not there in teratadata gcp side but present in edl
-- select edl.*
-- FROM `p-asna-analytics-002.edl_conform.transaction` edl
-- LEFT OUTER JOIN `work.transaction` edw 
-- on
-- edl.transaction_key =edw.transaction_key 
-- WHERE edl.transaction_dt > '2020-02-12' and edl.transaction_dt < '2020-02-22'
-- and edl.brand_cd ='LB'
-- and edw.transaction_key is null

--Check a transaction is present in staging or not
--select * from `edl_stage.plus_sales_transaction_header`   where transaction_dt='2020-02-06' and transaction_nbr=9025 and selling_store_nbr=4267 and register_nbr=1 and selling_chain_nbr=1

--check the transaction which are there teradata extract but not in EDL
-- SELECT sum(case when edw.transaction_key is null then 0 else 1 end)   as match_count
-- ,sum(case when edw.transaction_key is null then 1 else 0 end)     as orphan_count
-- FROM `work.transaction` edl
-- LEFT OUTER JOIN `p-asna-analytics-002.analytic_mart.fact_transaction` edw 
-- on
-- edl.transaction_key =edw.transaction_key 
-- WHERE edl.transaction_dt > '2020-02-12' and edl.transaction_dt < '2020-02-22'
-- and edl.brand_cd ='LB'

--Check the distinct count of transaction in staging table 

-- select count(*) from (
-- select transaction_nbr,
-- selling_store_nbr,
-- register_nbr,
-- selling_chain_nbr,
-- transaction_dt,
-- count(*)
-- from `edl_stage.plus_sales_transaction_header` where
-- transaction_dt > '2020-02-12' and transaction_dt < '2020-02-22'
-- and selling_chain_nbr =7
-- group by
-- transaction_nbr,
-- selling_store_nbr,
-- register_nbr,
-- selling_chain_nbr,
-- transaction_dt
-- );


--incremental comparision

select count(*) from work.fact_transaction --1425099

after incremental load  
select count(*) from work.fact_transaction --1,514,896

incremental load check

SELECT  edl.edl_last_update_tms,count(*)
FROM work.fact_transaction edl
LEFT OUTER JOIN `edl_archive.fact_transaction` edw 
on
edl.transaction_key =edw.transaction_key
where edw.transaction_key is null
group by edl.edl_last_update_tms

--	2020-03-05 10:57:03.232839 UTC 89797


---------------- Discount test cases  --------------------

 -- distinct count of discount in GCP side
  
  select count(*) from (
  SELECT
    transaction_key,
    transaction_line_num,
    discount_line_num 
  FROM
    `work.plus_transaction_discount`
  WHERE transaction_dt='2020-02-16'
  GROUP BY
    transaction_key,
    transaction_line_num,
    discount_line_num
  ) 
  
  
 --distinct count of discount in teradata  
  
  SELECT
  COUNT(*)
FROM (
  SELECT
    transaction_nbr,
    selling_store_nbr,
    selling_chain_nbr,
    register_nbr,
    transaction_dt,
    detail_fee_seq_nbr,
	discount_seq_nbr
  FROM
    EDWP_LB_RPT.SALES_TRANSACTION_discount
  GROUP BY
    transaction_nbr,
    selling_store_nbr,
    selling_chain_nbr,
    register_nbr,
    transaction_dt,
    detail_fee_seq_nbr,
	discount_seq_nbr
  WHERE
    transaction_dt='2020-02-16') AS a
	

--compare transaction discount between EDL GCP and Teradata GCP
SELECT
  SUM(CASE WHEN edw.transaction_key IS NULL THEN 0 ELSE 1 END) AS match_count,
  SUM(CASE WHEN edw.transaction_key IS NULL THEN 1 ELSE 0 END) AS orphan_count
FROM (
  SELECT
    a.transaction_key,
    a.transaction_dt,
    a.brand_cd,
    a.transaction_line_num
  FROM
    `p-asna-analytics-002.analytic_mart.fact_transaction_discount` a
  INNER JOIN (
    SELECT
      transaction_key,
      transaction_line_num
    FROM
      `p-asna-analytics-002.analytic_mart.fact_transaction_detail`
    WHERE
      transaction_dt = '2020-02-16'
      AND line_object_cd !='110'
    GROUP BY
      transaction_key,
      transaction_line_num ) b
  ON
    a.transaction_key =b.transaction_key
    AND a.transaction_line_num=b.transaction_line_num
  GROUP BY
    a.transaction_key,
    a.transaction_dt,
    a.brand_cd,
    a.transaction_line_num) AS edl  --edl 
LEFT OUTER JOIN (
  SELECT
    transaction_key,
    transaction_line_num
  FROM
    `work.plus_transaction_discount`
  GROUP BY
    transaction_key,
    transaction_line_num) edw --l
ON
  edl.transaction_key =edw.transaction_key
  AND CAST(edl.transaction_line_num AS INT64) = CAST(edw.transaction_line_num AS int64)
  --and cast(edl.discount_line_num AS INT64) = CAST(edw.discount_line_num AS INT64)
WHERE
  edl.transaction_dt = '2020-02-16'
  AND edl.brand_cd ='LB'
  
  
--ship_to_store_nbr is 0 only for ecom transaction  
select 	selling_store_nbr from `edl_stage.plus_sales_transaction_header`   where ship_to_store_nbr=0 group by selling_store_nbr --5000 and 6000 ,  ship_to_store_nbr is 0 only for ecom transaction