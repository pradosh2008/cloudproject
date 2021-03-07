CREATE OR REPLACE TABLE analytic_mart.pre_item AS 
SELECT  
lp.product_id
, pt.sku as item_key
, pt.style_key as style_key
, lp.style_desc as style_des
, pt.department_cd
, pt.department_des
, pt.class_cd
, pt.class_des
, pt.color_cd
, pt.color_des
, pt.size_cd
, pt.size_des
, pt.retail_price_amt
, pt.landed_cost_amt
, pt.currency_cd
, pt.original_retail_amt
, pt.batch_id
FROM (
select
sku
,cast(substr(article,1,15) as int64) as style_key
,dept as department_cd
,dept_desc as department_des
,class as class_cd
,class_desc as class_des
,color as color_cd
,color_desc as color_des
,size as size_cd
,size_desc as size_des
,retail_price as retail_price_amt
,landed_cost as landed_cost_amt
,currency as currency_cd
,original_retail as original_retail_amt
,batch_id
,row_number() over(partition by sku order by datetime desc) as rn 
FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_product`
) as pt
LEFT OUTER JOIN `p-asna-analytics-002.edl_landing.ann_hst_lu_product_vw` as lp ON (pt.sku = lp.sku AND pt.style_key = lp.style_nbr/*AND pt.class_cd = SUBSTR(CAST(lp.class_cd as STRING), -3) AND pt.department_cd = CAST(lp.dept_cd AS                                                                                       STRING) AND pt.color_cd = CAST(lp.color_cd AS STRING) AND pt.size_cd = CAST(lp.size_cd AS STRING) */
                                                                                   )
where pt.rn=1
UNION ALL
SELECT 
product_id 
, sku as item_key 
, style_nbr as style_key
, style_desc as style_des
, CAST(dept_cd AS STRING) as department_cd
, dept_desc as department_des
, SUBSTR(CAST(class_cd AS STRING), -3) as class_cd
, class_desc as class_des
, CAST(color_cd AS STRING) as color_cd
, color_desc as color_des
, CAST(size_cd AS STRING) as size_cd
, size_desc as size_des
, retail_price as retail_price_amt
, landed_cost as landed_cost_amt
, NULL as currency_cd
, original_retail as original_retail_amt
, batch_id
FROM `p-asna-analytics-002.edl_landing.ann_hst_lu_product_vw`
WHERE sku NOT IN (SELECT distinct sku FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_product`)
;

/*CREATE OR REPLACE TABLE work.wrk_ann_product AS 
SELECT  
lp.product_id
, pt.sku
, pt.style_key as style_cd
, lp.style_desc as style_des
, pt.department_cd
, pt.department_des
, pt.class_cd
, pt.class_des
, pt.color_cd
, pt.color_des
, pt.size_cd
, pt.size_des
, pt.retail_price_amt
, pt.landed_cost_amt
, pt.currency_cd
, pt.original_retail_amt
, pt.batch_id
FROM (
select
sku
,cast(substr(article,1,15) as int64) as style_key
,dept as department_cd
,dept_desc as department_des
,class as class_cd
,class_desc as class_des
,color as color_cd
,color_desc as color_des
,size as size_cd
,size_desc as size_des
,retail_price as retail_price_amt
,landed_cost as landed_cost_amt
,currency as currency_cd
,original_retail as original_retail_amt
,batch_id
,row_number() over(partition by sku order by datetime desc) as rn 
FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_product`
) as pt
LEFT OUTER JOIN `p-asna-analytics-002.edl_landing.ann_hst_lu_product_vw` as lp ON (pt.sku = lp.sku AND pt.style_key = lp.style_nbr
--AND pt.class_cd = SUBSTR(CAST(lp.class_cd as STRING), -3) AND pt.department_cd = CAST(lp.dept_cd AS STRING) AND pt.color_cd = CAST(lp.color_cd AS STRING) AND pt.size_cd = CAST(lp.size_cd AS STRING)
                                                                                   )
where pt.rn=1
;*/