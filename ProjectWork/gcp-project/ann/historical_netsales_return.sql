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
	s.style_nbr = pr.style_nbr where fiscal_week_id=201838 and l.store_nbr =1055 and pr.color_cd =7195 and s.style_nbr =319482


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
  where fiscal_week_id=201838 and l.store_nbr =1055 and pr.color_cd =7195 and s.style_nbr =319482
 group by fiscal_week_id,l.store_nbr,pr.color_cd,s.style_nbr ;
 
 --fiscal_week_id=201712 and l.store_nbr =805 and pr.color_cd =3786 and s.style_nbr =428448
 --fiscal_week_id=201843 and l.store_nbr =612 and pr.color_cd =3542 and s.style_nbr =483635 --- 
 
 need to sum adjusted
 net count does not match
 return count matches
 return amount matches
 			
