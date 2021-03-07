/*
select	EXTRACT(YEAR from eps.txn_dt) AS YEAR,EXTRACT(MONTH from eps.txn_dt) as MONTH,
		count(*)
from	edl_landing.f_txn_item as eps 
where	eps.txn_source_cd='ATG' 
	and eps.item_ret_qty != 0 
	and eps.txn_nbr  not like 'ORET%' 
	and EXTRACT(DATE FROM eps.txn_dt) >= '2018-01-01'
group by 1,2
order by 1,2
*/

--'2017-01-01'
 
 /*
 select	EXTRACT(YEAR from eps.txn_dt) AS YEAR,EXTRACT(MONTH from eps.txn_dt) as MONTH,
		count(*) as cnt
from	edl_landing.f_txn_item as eps 
where	eps.txn_source_cd='ATG' 
	and eps.item_ret_qty != 0 
	and eps.txn_nbr like 'ORET%' 
	and EXTRACT(DATE FROM eps.txn_dt) > '2017-01-01'
group by 1,2
order by 1,2
*/

/*
select 
DATE_TRUNC((cast (eps.txn_dt as date)),MONTH)
,sum(CASE WHEN atg.orderid is null then 0 else 1 end) match_in_atg
,sum(CASE WHEN atg.orderid is null then 1 else 0 end) mismatch_in_atg
,sum(CASE WHEN atg.orderid is null then 0 else 1 end)/count(txn_nbr) as perc_match
--from edl_landing.ann_hst_f_txn_header eps
from edl_landing.f_txn_item eps
left join
      (select distinct orderid
            from  edl_landing.ann_ann_atg_transactions
            --where PARSE_DATE('%Y/%m/%d',REGEXP_EXTRACT(submitteddate, r'(.*?):' ))  <  '2018-12-31' and PARSE_DATE('%Y/%m/%d',REGEXP_EXTRACT(submitteddate, r'(.*?):' )) > '2018-01-01'            
            ) atg
on eps.txn_nbr = atg.orderid
where eps.txn_source_cd = 'ATG' 
and eps.item_ret_qty != 0 
and EXTRACT(DATE FROM eps.txn_dt) > '2017-01-01'
group by 1
order by 1
*/
 
 --and EXTRACT(DATE FROM eps.txn_dt) > '2018-09-01' --89633539

/*
select 
DATE_TRUNC((cast (eps.txn_dt as date)),MONTH)
,sum(CASE WHEN inc.txn_nbr is null then 0 else 1 end) match_in_atg
,sum(CASE WHEN inc.txn_nbr is null then 1 else 0 end) mismatch_in_atg
,sum(CASE WHEN inc.txn_nbr is null then 0 else 1 end)/count(eps.txn_nbr) as perc_match
from edl_landing.ann_hst_f_txn_header eps
left join
(select txn_nbr from edl_landing.f_txn_item where 
txn_type_cd='RTN'
and txn_source_cd='ATG' 
--and item_ret_qty != 0 
and EXTRACT(DATE FROM txn_dt) >= '2018-01-01'
  group by 1) as inc
--on substr(eps.txn_nbr,1)=inc.txn_nbr
on eps.txn_nbr=inc.txn_nbr
where 
txn_type_cd='RTN'
AND eps.txn_source_cd = 'ATG' 
and EXTRACT(DATE FROM eps.txn_dt) > '2018-01-01'
group by 1
order by 1

*/
select 
store_id
,week_id
,style_id
,color_id
,sku_id
,transaction_date
,sum(quantity) as unit_cnt
,sum(gross_amt-discount_amt) as net_sales_amt
--, ROW_NUMBER() over (PARTITION BY inc.BATCH_ID, inc.ORDERID) RN
from 
(
select 
siteid as store_id
, cal.fiscal_week_id as week_id
, s.style_number as style_id
, substr(p.color,3,4) color_id 
, catalogrefid as sku_id
, PARSE_DATE('%Y/%m/%d',REGEXP_EXTRACT(submitteddate, r'(.*?):' )) as transaction_date
, orderid as orderid
, atg.BATCH_ID as batch_id
, max(cast(commerceitemquantity as INT64)) as quantity
, max(cast(commerceitempriceinfoamount as FLOAT64)) as gross_amt
, max(cast(detaileditempriceinfoorderdiscountshare as FLOAT64)) as discount_amt
, dense_rank() over (PARTITION BY orderid order by atg.batch_id desc) as rn
from   edl_landing.ann_ann_atg_transactions atg
LEFT JOIN (
               SELECT *
                 FROM (
                          (
                           SELECT *
                                , ROW_NUMBER()
                             OVER (
                            PARTITION BY sku
                                ORDER BY datetime DESC
                                  ) AS rn
                             FROM edl_landing.ann_ann_sap_product
                          )
                      )a
                WHERE rn=1
              )p
           ON atg.catalogrefid = p.sku
              LEFT JOIN edl_landing.ann_ann_sap_style s
                             ON s.style_number = substr(p.article, 10, 6)  
              LEFT JOIN edl_landing.ann_calendar cal
                             ON PARSE_DATE('%Y/%m/%d',REGEXP_EXTRACT(submitteddate, r'(.*?):' )) = CAST (day_dt AS date)
--WHERE RN= 1 
GROUP BY 1,2,3,4,5,6,7,8
)inc
where rn = 1 
--and orderid = '881263813241'
and orderid = '881164714672'
group by 1,2,3,4,5,6
--and sku_id = '21424335'