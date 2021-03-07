------------------  test query -------------------------
select
FARM_FINGERPRINT(CAST(class_id AS STRING)) as class_key
--class_id
--,department_id
--,division_id
--,chain_id
,b.brand_cd as brand_cd
,channel_id
,country_id
,week_id
,month_id
,sales_type_cd
--,count(*)
,op_net_sls_retail_amt
,op_net_sls_unit_cnt
,op_net_sls_aur_amt
,op_ttl_store_cnt
,op_comp_store_cnt
,op_bop_store_inv_retail_amt
,op_bop_store_inv_unit_cnt
,op_eop_store_inv_retail_amt
,op_eop_store_inv_unit_cnt
,cp_net_sls_retail_amt
,cp_net_sls_unit_cnt
,cp_net_sls_aur_amt
,cp_ttl_store_cnt
,cp_comp_store_cnt
,cp_bop_store_inv_retail_amt
,cp_bop_store_inv_unit_cnt
,cp_eop_store_inv_retail_amt
,cp_eop_store_inv_unit_cnt
from
(select *
from
(select 
*
,ROW_NUMBER() OVER(PARTITION BY class_id
,department_id
,division_id
,chain_id
,channel_id
,country_id
,week_id
,month_id
,sales_type_cd
ORDER BY batch_id DESC) AS rn
   FROM
    edl_stage.pre_sap_plan_data ) AS a
 WHERE
   rn=1 ) pd
left outer join
analytic_mart.pre_brand as b
on
cast(pd.chain_id as int64)=b.chain_id
--group by 1,2,3,4,5,6,7
--having count(*)>1
 
 
--------------------------------------- final query -------------------------------- 

create or replace table analytic_mart.pre_plan as
select
FARM_FINGERPRINT(CAST(class_id AS STRING)) as class_key
--class_id
--,department_id
--,division_id
--,chain_id
,b.brand_cd as brand_cd
,channel_id
,country_id
,week_id
,month_id
,sales_type_cd
--,count(*)
,op_net_sls_retail_amt
,op_net_sls_unit_cnt
,op_net_sls_aur_amt
,op_ttl_store_cnt
,op_comp_store_cnt
,op_bop_store_inv_retail_amt
,op_bop_store_inv_unit_cnt
,op_eop_store_inv_retail_amt
,op_eop_store_inv_unit_cnt
,cp_net_sls_retail_amt
,cp_net_sls_unit_cnt
,cp_net_sls_aur_amt
,cp_ttl_store_cnt
,cp_comp_store_cnt
,cp_bop_store_inv_retail_amt
,cp_bop_store_inv_unit_cnt
,cp_eop_store_inv_retail_amt
,cp_eop_store_inv_unit_cnt
from
(select *
from
(select 
*
,ROW_NUMBER() OVER(PARTITION BY class_id
,department_id
,division_id
,chain_id
,channel_id
,country_id
,week_id
,month_id
,sales_type_cd
ORDER BY batch_id DESC) AS rn
   FROM
    edl_stage.pre_sap_plan_data ) AS a
 WHERE
   rn=1 ) pd
left outer join
analytic_mart.pre_brand as b
on
cast(pd.chain_id as int64)=b.chain_id
--group by 1,2,3,4,5,6,7
--having count(*)>1