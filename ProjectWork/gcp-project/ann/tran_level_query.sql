SELECT
  /*h.if_entry_no,    
  transaction_nbr,
  e_item_sku,
  e_full_price_flg,  
  E_transaction_nbr,
  item_sku  ,
  full_price_flg,*/
  inc.if_entry_no,    
  item_sku,
  transaction_nbr,
  m.sku_id, 
   inc.store_no AS store_id,
	  cal.fiscal_week_id AS week_id,
	  s.style_number AS style_id,
	  p.color AS color_id,
    l.line_object_type ,
    l.line_Action,
  --ifnull(full_price_flg,e_full_price_flg) final_flag,
    (CASE WHEN l.line_Action = 1 THEN 1
			  WHEN l.line_Action = 2 THEN -1
		ELSE 0
	  END) AS Net_Sales_Unit_cnt,
	  ROUND((CASE
			WHEN l.line_Action = 1 THEN gross_line_amount-pos_discount_amount
			WHEN l.line_Action = 2 THEN (-1)*(gross_line_amount-pos_discount_amount)
		  ELSE      0
		END      )/100,2) AS Net_sales_amt,

	  (CASE      WHEN l.line_Action = 2 THEN 1
		ELSE    0
	  END    ) AS Return_Sales_Unit_cnt,
	  ROUND((CASE        WHEN l.line_Action = 2 THEN (gross_line_amount-pos_discount_amount)
		  ELSE      0
		END      )/100,2) AS return_sales_dollars_amt
	 FROM
  `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header` inc
   JOIN edl_landing.ann_ann_aw_transactions_line l
              ON inc.if_entry_no = l.if_entry_no
              AND l.line_object_type = 1
              AND line_void_flag = '0'
         left join edl_landing.ann_ann_aw_transactions_merchdtl m
              on m.if_entry_no = l.if_entry_no
              and l.line_id = m.line_id
         left JOIN edl_landing.ann_calendar cal
              ON transaction_Date = CAST (day_dt AS date)
         left JOIN (
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
           ON cast(m.sku_id as string) = p.sku
         left JOIN edl_landing.ann_ann_sap_style s
           ON s.style_number = substr(p.article, 10, 6)            
           LEFT JOIN ( SELECT *	FROM 
				(SELECT *,ifnull(full_price_flg,'N') as fpnfp_flag,
						ROW_NUMBER() OVER (PARTITION BY transaction_nbr, item_sku, full_price_flg, batch_id) AS rn
				FROM `p-asna-analytics-002.work.ann_ann_bi_transaction_fpnfp` ) a
				WHERE rn = 1 
			)bi
		ON  bi.item_sku = cast(m.sku_id as string)	
		AND bi.transaction_nbr = CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',SUBSTR(CAST(transaction_no AS string),1,4))
where 
	CONCAT(CAST(inc.store_no AS string)	,':'
	,CAST (inc.register_no AS string)	,':'
	,FORMAT_DATE('%Y%m%d',transaction_date), ':'
	,cast(transaction_no AS string)) 
	NOT IN 
			(select CONCAT(CAST(inc_20.store_no AS string),':'
					,CAST (inc_20.register_no AS string),':'
					,FORMAT_DATE('%Y%m%d',inc_20.transaction_date), ':'
					,cast(inc_20.transaction_no AS string)) 
			from edl_landing.ann_ann_aw_transactions_header inc_20  
			where inc_20.interface_control_flag  = 20)
and inc.store_no not in ( 611, 612, 613, 616, 617, 618 , 619)    --and fiscal_week_id = 201919
--and cal.fiscal_Week_id  = 201908 
and inc.store_no not in( 4000,4001,4002,4003,4100,4101,4102,4103,4104,4105,4106,4107,4108) --sample hist incre full match --< 201916 -- historical data limit 
 and s.style_number  not in ('999099','164562','91482') --, substr(p.color,3,4) color_id 
-- and  s.class <> '999'	
--where h.if_entry_no IN (1041625471,1039603579,1034757985,1040580356)
--where h.store_no = 1838 and fiscal_Week_id = 201913 and s.style_number like ('%494835%') and p.color like ('%4980')
and cal.fiscal_week_id=201837 and inc.store_no =674 and p.color like '%473' and s.style_number like'%483086';



-----------------------------------

select 
		store_nbr Store_ID
		,fiscal_week_id Week_ID
		,cast(s.style_nbr as string) Style_ID
		,pr.color_cd Color_ID
		,sku
		,extract (date from txn_dt) as Day_dt
    ,t.*
		,case when item_sold_qty>0  Then item_sold_qty else -item_ret_qty end as  Net_Sales_Unit_cnt
		,case when item_sold_qty>0 Then item_net_amt else -item_ret_amt end as  Net_Sales_Amt
		,case when item_sold_qty>0 Then round(item_cogs,2)*item_sold_qty else -round(item_cogs,2)*item_ret_qty end as  Net_Sales_COGS_amt
		,case when item_ret_qty>0 Then item_ret_qty else -item_ret_qty end as  Return_Sales_Unit_cnt
		,case when item_ret_qty>0 Then item_ret_amt else 0 end as  Return_Sales_Dollars_amt
		,case when item_ret_qty>0 Then round(item_cogs,2)*item_ret_qty else 0 end as  Return_Sales_COGS_amt
		,case when t.FULL_PRICE_FLG = 'N' and item_sold_qty > 0  THEN item_sold_qty 
				when t.FULL_PRICE_FLG = 'N' and item_ret_qty > 0  THEN -item_ret_qty 
				else 0 
		end  as Markdown_Net_Sales_Unit_cnt
		,case when t.FULL_PRICE_FLG = 'N' and item_sold_qty > 0  THEN item_net_amt
				when t.FULL_PRICE_FLG = 'N' and item_ret_qty > 0  THEN -item_ret_amt 
				else 0 
		end  as Markdown_Net_Sales_Dollars_amt      
		,case when t.FULL_PRICE_FLG = 'N'  and item_sold_qty > 0 THEN round(item_cogs,2)
				when t.FULL_PRICE_FLG = 'N' and item_ret_qty > 0  THEN -round(item_cogs,2) 
				else 0 
		end  as Markdown_Net_Sales_COGS_amt            
		,CASE when item_ret_qty>0 And full_price_flg='F' then -item_ret_qty
				when item_sold_qty>0 And full_price_flg='F' then item_sold_qty
				else 0 
		end as Promotion_Net_Sales_Unit_cnt
		,CASE when item_ret_qty>0 And full_price_flg='F' then -round(item_ret_amt,2)
				when item_sold_qty>0 And full_price_flg='F' then round(item_net_amt,2)
				else 0 
		end as Promotion_Net_Sales_Dollars_amt      
		,CASE when item_ret_qty>0 And full_price_flg='F' then -round(item_cogs,2)
				when item_sold_qty>0 And full_price_flg='F' then round(item_cogs,2)
				else 0 
		end as Promotion_Net_Sales_COGS_amt
FROM work.ann_hst_f_txn_item_2016 t
left join edl_landing.ann_calendar cal
  on CAST (day_dt AS date) = EXTRACT(date FROM txn_dt) 
left join edl_landing.ann_hst_lu_product_vw pr
  on pr.product_id = t.product_id 
left JOIN edl_landing.ann_hst_lu_style_vw s
	  ON s.style_nbr = pr.style_nbr 
where    s.style_nbr = 483086 and store_nbr = 674 and fiscal_Week_id = 201837
and color_Cd = 473


------------------------------------

-- without markdown and promotion - not joining ann_ann_bi_transaction_fpnfp -- updated query



SELECT
  /*h.if_entry_no,    
  transaction_nbr,
  e_item_sku,
  e_full_price_flg,  
  E_transaction_nbr,
  item_sku  ,
  full_price_flg,*/
  inc.store_no AS store_id,
	cal.fiscal_week_id AS week_id,
	s.style_number AS style_id,
	p.color AS color_id,
  m.sku_id, 
  inc.if_entry_no, 
  CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',CAST(transaction_no AS string)) as transaction_nbr,
  transaction_date,
  register_no,
  transaction_no,
  l.line_id ,
  l.line_object_type ,
  l.line_Action,
  l.gross_line_amount ,
  l.pos_discount_amount ,
  --ifnull(full_price_flg,e_full_price_flg) final_flag,
    (CASE WHEN l.line_Action = 1 THEN 1
			  WHEN l.line_Action = 2 THEN -1
		ELSE 0
	  END) AS Net_Sales_Unit_cnt,
	  ROUND((CASE
			WHEN l.line_Action = 1 THEN gross_line_amount-pos_discount_amount
			WHEN l.line_Action = 2 THEN (-1)*(gross_line_amount-pos_discount_amount)
		  ELSE      0
		END      )/100,2) AS Net_sales_amt,

	  (CASE      WHEN l.line_Action = 2 THEN 1
		ELSE    0
	  END    ) AS Return_Sales_Unit_cnt,
	  ROUND((CASE        WHEN l.line_Action = 2 THEN (gross_line_amount-pos_discount_amount)
		  ELSE      0
		END      )/100,2) AS return_sales_dollars_amt
	 FROM
  `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header` inc
   JOIN edl_landing.ann_ann_aw_transactions_line l
              ON inc.if_entry_no = l.if_entry_no
              AND l.line_object_type = 1
              AND line_void_flag = '0'
         left join edl_landing.ann_ann_aw_transactions_merchdtl m
              on m.if_entry_no = l.if_entry_no
              and l.line_id = m.line_id
         left JOIN edl_landing.ann_calendar cal
              ON transaction_Date = CAST (day_dt AS date)
         left JOIN (
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
           ON cast(m.sku_id as string) = p.sku
         left JOIN edl_landing.ann_ann_sap_style s
           ON s.style_number = substr(p.article, 10, 6)            
 /*          LEFT JOIN ( SELECT *	FROM 
				(SELECT *,ifnull(full_price_flg,'N') as fpnfp_flag,
						ROW_NUMBER() OVER (PARTITION BY transaction_nbr, item_sku, full_price_flg, batch_id) AS rn
				FROM `p-asna-analytics-002.work.ann_ann_bi_transaction_fpnfp` ) a
				WHERE rn = 1 
			)bi
		ON  bi.item_sku = cast(m.sku_id as string)	
		AND bi.transaction_nbr = CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',SUBSTR(CAST(transaction_no AS string),1,4)) */
where 
	CONCAT(CAST(inc.store_no AS string)	,':'
	,CAST (inc.register_no AS string)	,':'
	,FORMAT_DATE('%Y%m%d',transaction_date), ':'
	,cast(transaction_no AS string)) 
	NOT IN 
			(select CONCAT(CAST(inc_20.store_no AS string),':'
					,CAST (inc_20.register_no AS string),':'
					,FORMAT_DATE('%Y%m%d',inc_20.transaction_date), ':'
					,cast(inc_20.transaction_no AS string)) 
			from edl_landing.ann_ann_aw_transactions_header inc_20  
			where inc_20.interface_control_flag  = 20)
and inc.store_no not in ( 611, 612, 613, 616, 617, 618 , 619)    --and fiscal_week_id = 201919
--and cal.fiscal_Week_id  = 201908 
and inc.store_no not in( 4000,4001,4002,4003,4100,4101,4102,4103,4104,4105,4106,4107,4108) --sample hist incre full match --< 201916 -- historical data limit 
 and s.style_number  not in ('999099','164562','91482') --, substr(p.color,3,4) color_id 
-- and  s.class <> '999'	
--where h.if_entry_no IN (1041625471,1039603579,1034757985,1040580356)
--where h.store_no = 1838 and fiscal_Week_id = 201913 and s.style_number like ('%494835%') and p.color like ('%4980')
and cal.fiscal_week_id=201908 and inc.store_no =663 and p.color like '%473' and s.style_number like'%497348'




----------------------------------


--updated tran level query for hisotry

select 
		store_nbr Store_ID
		,fiscal_week_id Week_ID
		,cast(s.style_nbr as string) Style_ID
		,pr.color_cd Color_ID
		,sku
		,extract (date from txn_dt) as Day_dt
    ,t.txn_time
    ,t.txn_nbr
    ,t.txn_source_cd
    ,t.brand_cd
    ,t.txn_item_nbr
,t.register_nbr
,t.txn_type_cd
,t.txn_channel_cd
,t.txn_status_cd
,t.product_id
,t.item_list_price
,t.item_sold_qty
,t.item_sold_price
,t.item_cogs
,t.item_gross_amt
,t.item_disc_amt
,t.item_giftcard_amt
,t.item_adj_cd
,t.item_adj_amt
,t.item_net_amt
,t.item_ret_from_txn_id
,t.item_ret_cd
,t.item_ret_dt
,t.item_ret_qty
,t.item_ret_amt
,t.full_price_ind
,t.item_ret_from_store_nbr
,t.originoforder
,t.source_keycode
,t.full_price_flg
,t.pickup_type
,t.pickup_store_nbr
,t.partner_cd
,t.bopis_item_adj_cd
		,case when item_sold_qty>0  Then item_sold_qty else -item_ret_qty end as  Net_Sales_Unit_cnt
		,case when item_sold_qty>0 Then item_net_amt else -item_ret_amt end as  Net_Sales_Amt
		,case when item_sold_qty>0 Then round(item_cogs,2)*item_sold_qty else -round(item_cogs,2)*item_ret_qty end as  Net_Sales_COGS_amt
		,case when item_ret_qty>0 Then item_ret_qty else -item_ret_qty end as  Return_Sales_Unit_cnt
		,case when item_ret_qty>0 Then item_ret_amt else 0 end as  Return_Sales_Dollars_amt
		,case when item_ret_qty>0 Then round(item_cogs,2)*item_ret_qty else 0 end as  Return_Sales_COGS_amt
		,case when t.FULL_PRICE_FLG = 'N' and item_sold_qty > 0  THEN item_sold_qty 
				when t.FULL_PRICE_FLG = 'N' and item_ret_qty > 0  THEN -item_ret_qty 
				else 0 
		end  as Markdown_Net_Sales_Unit_cnt
		,case when t.FULL_PRICE_FLG = 'N' and item_sold_qty > 0  THEN item_net_amt
				when t.FULL_PRICE_FLG = 'N' and item_ret_qty > 0  THEN -item_ret_amt 
				else 0 
		end  as Markdown_Net_Sales_Dollars_amt      
		,case when t.FULL_PRICE_FLG = 'N'  and item_sold_qty > 0 THEN round(item_cogs,2)
				when t.FULL_PRICE_FLG = 'N' and item_ret_qty > 0  THEN -round(item_cogs,2) 
				else 0 
		end  as Markdown_Net_Sales_COGS_amt            
		,CASE when item_ret_qty>0 And full_price_flg='F' then -item_ret_qty
				when item_sold_qty>0 And full_price_flg='F' then item_sold_qty
				else 0 
		end as Promotion_Net_Sales_Unit_cnt
		,CASE when item_ret_qty>0 And full_price_flg='F' then -round(item_ret_amt,2)
				when item_sold_qty>0 And full_price_flg='F' then round(item_net_amt,2)
				else 0 
		end as Promotion_Net_Sales_Dollars_amt      
		,CASE when item_ret_qty>0 And full_price_flg='F' then -round(item_cogs,2)
				when item_sold_qty>0 And full_price_flg='F' then round(item_cogs,2)
				else 0 
		end as Promotion_Net_Sales_COGS_amt
FROM work.ann_hst_f_txn_item_2016 t
left join edl_landing.ann_calendar cal
  on CAST (day_dt AS date) = EXTRACT(date FROM txn_dt) 
left join edl_landing.lu_product_vw pr
  on pr.product_id = t.product_id 
left JOIN edl_landing.lu_style_vw s
	  ON s.style_nbr = pr.style_nbr 
where    s.style_nbr = 497348 and store_nbr = 663 and fiscal_Week_id = 201908
and color_Cd = 473





