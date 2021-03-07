select	
	a.store_id
	,a.week_id
	,a.style_id
	,a.color_id
	,a.Net_Sales_Unit_cnt
	,a.Net_sales_amt
	,cogs.Net_Sales_COGS_amt as Net_sales_cogs_amt
	,a.Return_Sales_Unit_cnt
	,a.return_sales_dollars_amt
	,cogs.Return_Sales_COGS_amt as Return_Sales_COGS_amt
	from	
	(
	SELECT
	      inc.store_no AS store_id,
		  cal.fiscal_week_id AS week_id,
		  s.style_number AS style_id,
		  p.color AS color_id
	     ,sum(
			CASE
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
	          )                                                         AS net_sales_unit_cnt
	        ,ROUND(sum(
			CASE
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
	              )/100, 2)                                             AS net_sales_amt
	
	            ,SUM(
			CASE 
					  WHEN l.line_Action = 1 
				and gross_line_amount < 0   THEN 1
	                  WHEN l.line_Action = 2 
				and gross_line_amount >= 0  THEN 1                
	                  ELSE    0
	                  END ) 											AS return_sales_unit_cnt
	            ,ROUND(sum(
			CASE        
	                  WHEN l.line_Action = 1 
				and gross_line_amount < 0  THEN (gross_line_amount-pos_discount_amount) 
	                  WHEN l.line_Action = 2 
				and gross_line_amount >= 0 THEN (gross_line_amount-pos_discount_amount)
					  ELSE      0
					  END      )/100,2)                                 AS return_sales_dollars_amt	  	              
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
		               SELECT	*
		                 FROM	(
		                          (
			                           SELECT	*
			                                , ROW_NUMBER()
			                             OVER (
			                            PARTITION BY sku
			                                ORDER BY datetime DESC
			                                  ) AS rn
			                             FROM	edl_landing.ann_ann_sap_product
			                          )
		                      )a
		                WHERE	rn=1
		              )p
	           ON cast(m.sku_id as string) = p.sku
	         left JOIN edl_landing.ann_ann_sap_style s
	           ON s.style_number = substr(p.article, 10, 6)            
	where	
		CONCAT(CAST(inc.store_no AS string)	,':'
		,CAST (inc.register_no AS string)	,':'
		,FORMAT_DATE('%Y%m%d',transaction_date), ':'
		,cast(transaction_no AS string)) 
		NOT IN 
				(
		select	CONCAT(CAST(inc_20.store_no AS string),':'
							,CAST (inc_20.register_no AS string),':'
							,FORMAT_DATE('%Y%m%d',inc_20.transaction_date), ':'
							,cast(inc_20.transaction_no AS string)) 
					from	edl_landing.ann_ann_aw_transactions_header inc_20  
					where	inc_20.interface_control_flag  = 20)
		and inc.store_no not in ( 611, 612, 613, 616, 617, 618 , 619) 
		and inc.store_no not in( 4000,4001,4002,4003,4100,4101,4102,4103,4104,4105,4106,4107,4108)
		and s.style_number  not in ('999099','164562','91482') 
	group by 
	inc.store_no,cal.fiscal_week_id, s.style_number,p.color
)  a
 LEFT JOIN   
 (
	SELECT	sum(Net_Sales_COGS_amt) as Net_Sales_COGS_amt, sum(Return_Sales_COGS_amt) as Return_Sales_COGS_amt,
			Store_ID,
			Week_ID,
			Style_ID,
			Color_ID 
	FROM	`p-asna-analytics-002.demand_forecast.sales_sum_settled` 
	group by Store_ID,
			Week_ID,
			Style_ID,
			Color_ID) as cogs
 on
	  cogs.store_id = a.store_id
	    AND cogs.week_id = a.week_id
		AND cast(cogs.Style_ID as INT64)  = cast(a.style_id as INT64)
		AND cast(cogs.color_id as INT64) = CAST(a.color_id as INT64)