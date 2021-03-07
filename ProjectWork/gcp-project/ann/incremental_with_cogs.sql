SELECT a.* FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_merchdtl` as a left outer join edl_landing.ann_ann_sap_product as b on  a.sku_id =cast(b.sku as int64) where b.sku is null;



SELECT l.store_nbr , cal.fiscal_week_id AS week_id ,s.style_nbr,pr.color_cd   FROM `work.ann_hst_f_txn_item_2016` as l JOIN
		edl_landing.ann_calendar cal
	  ON
		extract(date from txn_dt) = CAST (day_dt AS date)  
    join edl_landing.ann_hst_lu_product_vw pr
  on pr.product_id = l.product_id
  left JOIN
	edl_landing.ann_hst_lu_style_vw s
	  ON
	s.style_nbr = pr.style_nbr 
  where fiscal_week_id=201712 and l.store_nbr =805 and pr.color_cd =3786 and s.style_nbr =428448;
  

  
  \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
  
-- 1ST MATCHING CASE  
  
SELECT fiscal_week_id,l.store_nbr,pr.color_cd,s.style_nbr,
    SUM(CASE WHEN l.txn_type_cd = 'PUR' THEN 1
			  WHEN l.txn_type_cd = 'RTN' THEN -1
		ELSE 0
	  END) AS Net_Sales_Unit_cnt,
	  ROUND(SUM(CASE
			WHEN l.txn_type_cd = 'PUR' THEN l.item_net_amt
			WHEN l.txn_type_cd = 'RTN' THEN (-1)*(item_ret_amt)
		  ELSE      0
		END      ),2) AS Net_sales_amt,

	  SUM(CASE WHEN l.txn_type_cd = 'RTN' THEN 1
		ELSE    0
	  END    ) AS Return_Sales_Unit_cnt,
	  ROUND(SUM(CASE        WHEN l.txn_type_cd = 'RTN' THEN l.item_ret_amt
		  ELSE      0
		END      ),2) AS return_sales_dollars_amt
FROM `work.ann_hst_f_txn_item_2016`  as l JOIN
		edl_landing.ann_calendar cal
	  ON
		extract(date from txn_dt) = CAST (day_dt AS date)  
    join edl_landing.ann_hst_lu_product_vw pr
  on pr.product_id = l.product_id
  left JOIN
	edl_landing.ann_hst_lu_style_vw s
	  ON
	s.style_nbr = pr.style_nbr 
  where fiscal_week_id=201712 and l.store_nbr =805 and pr.color_cd =3786 and s.style_nbr =428448
 group by fiscal_week_id,l.store_nbr,pr.color_cd,s.style_nbr
 
 

\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

-----
  
SELECT fiscal_week_id
,l.store_nbr
,pr.color_cd
,s.style_nbr
 ,l.txn_type_cd 
,l.txn_channel_cd 
,l.txn_status_cd 
,l.item_sold_qty
,l.item_sold_price
,l.item_cogs
,l.item_gross_amt
,l.item_disc_amt
,l.item_giftcard_amt
,l.item_adj_cd
,l.item_adj_amt
,l.item_net_amt
,l.item_ret_from_txn_id
,l.item_ret_cd
,l.item_ret_dt
,l.item_ret_qty
,l.item_ret_amt
,l.full_price_ind FROM `work.ann_hst_f_txn_item_2016` as l JOIN
		edl_landing.ann_calendar cal
	  ON
		extract(date from txn_dt) = CAST (day_dt AS date)  
    join edl_landing.ann_hst_lu_product_vw pr
  on pr.product_id = l.product_id
  left JOIN
	edl_landing.ann_hst_lu_style_vw s
	  ON
	s.style_nbr = pr.style_nbr where fiscal_week_id=201801 and l.store_nbr =611 and pr.color_cd =22 and s.style_nbr =463410




//////////////////////////////////////////

SELECT fiscal_week_id,l.store_nbr,pr.color_cd,s.style_nbr,
    SUM(CASE WHEN l.item_ret_qty = '0' THEN 1
			  WHEN l.item_ret_qty !=0 'RTN' THEN -1
		ELSE 0
	  END) AS Net_Sales_Unit_cnt,
	  ROUND(SUM(CASE
			WHEN l.item_ret_qty = '0' THEN l.item_net_amt
			WHEN l.item_ret_qty !=0 THEN (-1)*(item_ret_amt)
		  ELSE      0
		END      ),2) AS Net_sales_amt,

	  SUM(CASE WHEN l.item_ret_qty !=0 THEN 1
		ELSE    0
	  END    ) AS Return_Sales_Unit_cnt,
	  ROUND(SUM(CASE        WHEN l.item_ret_qty !=0 THEN l.item_ret_amt
		  ELSE      0
		END      ),2) AS return_sales_dollars_amt
FROM `work.ann_hst_f_txn_item_2016`  as l JOIN
		edl_landing.ann_calendar cal
	  ON
		extract(date from txn_dt) = CAST (day_dt AS date)  
    join edl_landing.ann_hst_lu_product_vw pr
  on pr.product_id = l.product_id
  left JOIN
	edl_landing.ann_hst_lu_style_vw s
	  ON
	s.style_nbr = pr.style_nbr 
  where fiscal_week_id=201801 and l.store_nbr =612 and pr.color_cd =6 and s.style_nbr =463530
 group by fiscal_week_id,l.store_nbr,pr.color_cd,s.style_nbr ;
 
 --fiscal_week_id=201838 and l.store_nbr =1055 and pr.color_cd =7195 and s.style_nbr =319482
 --fiscal_week_id=201712 and l.store_nbr =805 and pr.color_cd =3786 and s.style_nbr =428448
 --fiscal_week_id=201843 and l.store_nbr =612 and pr.color_cd =3542 and s.style_nbr =483635 --- 201801	612	6	463530

 
 need to sum adjusted
 net count does not match
 return count matches
 return amount matches
 
\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

SELECT fiscal_week_id,l.store_nbr,pr.color_cd,s.style_nbr,
    SUM(CASE WHEN l.item_sold_qty > 0 THEN item_sold_qty else item_ret_qty end ) AS Net_Sales_Unit_cnt,
	  ROUND(SUM(CASE
			WHEN l.item_sold_qty > 0 THEN l.item_net_amt else (-1)*(item_ret_amt) END),2) AS Net_sales_amt,
	  SUM(CASE WHEN l.item_ret_qty > 0 THEN item_ret_qty else 0
	  END) AS Return_Sales_Unit_cnt,
	  ROUND(SUM(CASE WHEN l.item_ret_qty > 0 then l.item_ret_amt else 0 END),2) AS return_sales_dollars_amt
FROM `work.ann_hst_f_txn_item_2016`  as l JOIN
		edl_landing.ann_calendar cal
	  ON
		extract(date from txn_dt) = CAST (day_dt AS date)  
    join edl_landing.ann_hst_lu_product_vw pr
  on pr.product_id = l.product_id
  left JOIN
	edl_landing.ann_hst_lu_style_vw s
	  ON
	s.style_nbr = pr.style_nbr 
  where (fiscal_week_id>= 201801  and fiscal_week_id< 201802) and ( l.store_nbr >= 611 and l.store_nbr < 618 )
 group by fiscal_week_id,l.store_nbr,pr.color_cd,s.style_nbr ; 


\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

SELECT 
l.txn_nbr
,l.txn_dt
,l.txn_type_cd 
,l.txn_channel_cd 
,l.txn_status_cd
,fiscal_week_id
,l.store_nbr
,pr.color_cd
,pr.style_nbr
 ,l.item_sold_qty
,l.item_sold_price
,l.item_cogs
,l.item_gross_amt
,l.item_disc_amt
,l.item_giftcard_amt
,l.item_adj_cd
,l.item_adj_amt
,l.item_net_amt
,l.item_ret_from_txn_id
,l.item_ret_cd
,l.item_ret_dt
,l.item_ret_qty
,l.item_ret_amt
,l.full_price_ind FROM `work.ann_hst_f_txn_item_2016` as l JOIN
		edl_landing.ann_calendar cal
	  ON
		extract(date from txn_dt) = CAST (day_dt AS date)  
    inner join edl_landing.lu_product_vw pr
  on pr.product_id = l.product_id
 where cal.fiscal_week_id=201841 and l.store_nbr =612 and pr.color_cd =2222 and pr.style_nbr =483527
		
\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
2979002
2978893

-- 201843 2018-11-25  to 2018-12-01
-- 201842 2018-11-24  to 2018-11-18

select
edl_landing.ann_hst_lu_style_vw
where 
,'26043067'
,'26043074'
,'26043081'
,'26043098'
,'26044507'
,'26044514'

,3019225
,1011253
,3017169
,3017004
,3019192
,3017074


select distinct orderid
,createddate,    submitteddate, lastmodifieddate,completeddate,commerceitemstate
,paymentgroupsubmitteddate,paymentgroupstate
,shipondate
,actualshipdate
,shippinggroupstate,shippinggroupsubmitteddate
,shipitemrelstate
 from edl_landing.ann_ann_atg_transactions
where orderid = '11756839022';

select *, REGEXP_EXTRACT(line_note, r'(.*?)~') order_id     
,  REGEXP_EXTRACT(line_note, r'~([^~]+)~?$') order_date    
from   edl_landing.ann_ann_aw_transactions_linenotes l
where  line_note_type = 78
  and REGEXP_EXTRACT(line_note, r'(.*?)~') like '%11756839022%'

select txn_id,txn_item_id,txn_nbr  from edl_landing.ann_hst_f_txn_item where  txn_nbr = '11756839022' --520323534
--1591792304
--11756839022
select * from edl_landing.ann_hst_f_txn_header where  txn_nbr = '11756839022'

select 
fiscal_week_id, CAST(day_dt AS date) from edl_landing.ann_calendar where CAST(day_dt AS date)='2018-11-25';

-----------------------------------------------------------------



select	* 
from	 `edl_landing.ann_ann_aw_transactions_header` as inc 
where	 CONCAT(CAST(inc.store_no AS string),
		':',CAST (inc.register_no AS string),
		':',FORMAT_DATE('%Y%m%d',
		transaction_date),
		':',
		CAST(transaction_no AS string)) in ('3138:11:20190427:11'
,'3138:11:20190427:3'
,'3138:1:20190427:4'
,'3138:11:20190427:4'
,'3138:3:20190427:3')


 select * from edl_landing.ann_hst_f_txn_header where txn_nbr in ('3138:11:20190427:11'
,'3138:11:20190427:3'
,'3138:1:20190427:4'
,'3138:11:20190427:4'
,'3138:3:20190427:3')  


select * from  `edl_landing.f_txn_item`  where txn_nbr in ('3138:11:20190427:11'
,'3138:11:20190427:3'
,'3138:1:20190427:4'
,'3138:11:20190427:4'
,'3138:3:20190427:3')



'2542:1:20190217:7442'
,'2542:11:20190219:6796'
,'2542:1:20190218:7467'


'2542:11:20190219:6796'
,'2542:1:20190218:7467'

/data/pre/


'2958:3:20190409:7991'
,'2958:3:20190413:8117'
,'2958:2:20190411:10636'


'1913:2:20181201:8356'
,'1913:2:20181125:7885'

'2542:12:20190217:3314'
,'2542:11:20190217:6689'

'1921:1:20190203:69'
,'1921:1:20190203:30'



'881093053831'
,'881107748256'
,'5882782480'
,'881102793730'
,'881098688160'
,'11711065146'
,'881102946197'
,'881105573342'
,'881112981633'
,'881105773794'
