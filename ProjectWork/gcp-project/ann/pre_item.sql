CREATE or replace table work.pre_item
as
select
item_key
,style_key
,department_cd
,department_des
,class_cd
,class_des
,color_cd
,color_des
,size_cd
,size_des
,retail_price_amt
,landed_cost_amt
,currency_cd
,original_retail_amt
,batch_id
from (
select
sku as item_key
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
,row_number() over(partition by sku 
                           order by datetime desc
                        ) as rn FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_product`) as a where rn=1 ;
					