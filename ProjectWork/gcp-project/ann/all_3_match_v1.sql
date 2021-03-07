
select 
bi.week_id,
bi.store_id,
substr(bi.style_id,13,6)style_id,
substr(bi.color_id,3,4)color_id,
sum(bi.Net_Sales_Unit_cnt) BI_Net_Sales_Unit_cnt,
round(sum(bi.Net_sales_amt),2)BI_Net_sales_amt,
sum(incr.Net_Sales_Unit_cnt) incr_Net_Sales_Unit_cnt,
round(sum(incr.Net_sales_amt),2)incr_Net_sales_amt,
sum(hist.Net_Sales_Unit_cnt) hist_Net_Sales_Unit_cnt,
round(sum(hist.Net_sales_amt),2)hist_Net_sales_amt
from  demand_forecast.sales_sum_settled  bi
left join 
		( -- incremental
			select 
				cal.fiscal_week_id AS week_id,
				inc.store_no AS store_id,
				s.style_number AS style_id,
				substr(p.color,3,4) AS color_id  
				,sum(CASE
					  WHEN l.line_Action = 1
				  and gross_line_amount >= 0 THEN 1
					  WHEN l.line_Action = 2
				  and gross_line_amount < 0 THEN 1
					  WHEN l.line_Action = 1
				  and gross_line_amount < 0 THEN -1
					  WHEN l.line_Action = 2
				  and gross_line_amount >= 0 THEN -1
					  ELSE 0
					  END
				  )                                                         AS Net_Sales_Unit_cnt
				,ROUND(sum(CASE
						  WHEN l.line_Action = 1
					  and gross_line_amount >= 0 THEN gross_line_amount-pos_discount_amount
						  WHEN l.line_Action = 2
					  and gross_line_amount < 0 THEN gross_line_amount-pos_discount_amount
						  WHEN l.line_Action = 1
					  and gross_line_amount < 0 THEN (-1)*(gross_line_amount-pos_discount_amount)
						  WHEN l.line_Action = 2
					  and gross_line_amount >= 0 THEN (-1)*(gross_line_amount-pos_discount_amount)
						  ELSE 0
						  END
					  )/100, 2)                                             AS Net_sales_amt
					  
				,SUM
						(CASE 	WHEN l.line_Action = 1 
								and gross_line_amount < 0   THEN 1
								WHEN l.line_Action = 2 
								and gross_line_amount >= 0  THEN 1                
								ELSE    0
						END ) 												AS Return_Sales_Unit_cnt
				,ROUND(sum
						(CASE        
								WHEN l.line_Action = 1 
								and gross_line_amount < 0  THEN (gross_line_amount-pos_discount_amount) 
								WHEN l.line_Action = 2 
								and gross_line_amount >= 0 THEN (gross_line_amount-pos_discount_amount)
								ELSE      0
						END      )/100,2)                                 AS return_sales_dollars_amt	
				FROM 
				(
				select if_entry_no,store_no,entry_date_time from (
SELECT if_entry_no, store_no ,entry_date_time,
CASE WHEN interface_control_flag = 30 OR interface_control_flag = 10 THEN CONCAT(CAST(store_no AS string),':',CAST (register_no AS string),':',CAST(EXTRACT (DATE FROM (PARSE_DATETIME('%m/%d/%Y %H:%M:%S', entry_date_time))) AS STRING), ':',CAST(transaction_no AS string)) 
WHEN interface_control_flag IN (10, 20) THEN NULL
ELSE NULL END AS txn_nbr
, interface_control_flag
, rank() OVER (PARTITION BY store_no, register_no, transaction_date, transaction_no ORDER BY CASE WHEN interface_control_flag = 30 THEN 1
                                                                                                      WHEN interface_control_flag = 20 THEN 2
                                                                                                      ELSE 3 END) AS rn
FROM edl_landing.ann_ann_aw_transactions_header
) AS A WHERE rn = 1 
AND txn_nbr IS NOT NULL 
AND interface_control_flag != 20 ) as inc
					JOIN edl_landing.ann_ann_aw_transactions_line l
						  ON inc.if_entry_no = l.if_entry_no
						  AND l.line_object_type = 1
						  AND line_void_flag = '0'
					 left join edl_landing.ann_ann_aw_transactions_merchdtl m
						  on m.if_entry_no = l.if_entry_no
						  and l.line_id = m.line_id
					 left JOIN edl_landing.ann_calendar cal
						  ON EXTRACT (DATE FROM (PARSE_DATETIME('%m/%d/%Y %H:%M:%S', entry_date_time)))  = CAST (day_dt AS date)
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
				where 
				inc.store_no not in ( 611, 612, 613, 616, 617, 618 , 619)    --taking only POS
				and inc.store_no not in( 4000,4001,4002,4003,4100,4101,4102,4103,4104,4105,4106,4107,4108)  -- canada stores not in BI
				and s.style_number  not in ('999099','164562','91482') --dummy styles not in BI
				and cal.fiscal_week_id > 201831 
				group by 1,2,3,4  
		)incr
		on incr.week_id = bi.week_id
		and incr.store_id = bi.store_id
		and incr.style_id = substr(bi.style_id,13,6) 
		and incr.color_id = substr(bi.color_id,3,4)
left join 
		(-- historical
		SELECT 					
				fiscal_week_id hist_Week_ID
				,hist.store_nbr hist_store_id         
				,cast(s.style_nbr as string) hist_Style_ID
				,cast(pr.color_cd as string) hist_Color_ID					
				,sum(case when item_sold_qty>0  Then item_sold_qty else -item_ret_qty end) as  Net_Sales_Unit_cnt
		        ,round(sum(case when item_sold_qty>0 Then item_net_amt else -item_ret_amt end),2) as  Net_Sales_Amt            
		  FROM `p-asna-analytics-002.edl_landing.ann_hst_f_txn_header` hist
          join edl_landing.f_txn_item t
             on hist.txn_nbr = t.txn_nbr
          left join edl_landing.ann_calendar cal
            on CAST (day_dt AS date) = EXTRACT(date FROM hist.txn_dt)
          left join edl_landing.lu_product_vw pr
            on pr.product_id = t.product_id
          left JOIN edl_landing.lu_style_vw s
                ON s.style_nbr = pr.style_nbr                
				where HIST.txn_source_cd ='AW'  --and cal.fiscal_week_id > 201831 
			group by 1,2,3,4
		)hist
		on hist.hist_Week_ID = bi.week_id
		and hist.hist_store_id = bi.store_id
		and hist.hist_Style_ID = substr(bi.style_id,13,6) 
		and hist.hist_Color_ID = substr(bi.color_id,3,4)		
--where bi.store_id not in ( 611, 612, 613, 616, 617, 618 , 619)  
and bi.week_id > 201831 
group by 1,2,3,4
having 

abs(sum(BI.Net_Sales_Unit_cnt- incr.Net_Sales_Unit_cnt))  = 0 
and abs(sum(BI.Net_Sales_Unit_cnt- hist.Net_Sales_Unit_cnt))  >0

