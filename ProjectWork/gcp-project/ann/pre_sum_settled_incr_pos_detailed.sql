CREATE OR REPLACE TABLE work.pre_sum_settled_incr_pos_detailed as 
select a.*,ifnull(round(unit_cogs_amt,2),0) as unit_cogs_amt  from
(select --'Incremental' as source
		inc.store_no  as store_id
		,cal.fiscal_week_id as week_id
		,s.style_number style_id
		,substr(p.color,3,4) color_id  
		,p.sku
		,EXTRACT(DATE FROM (PARSE_DATETIME('%m/%d/%Y %H:%M:%S',entry_date_time))) as transaction_date
		,case 
			when PARSE_DATE('%m/%d/%Y',REGEXP_EXTRACT(ln.line_note, r'~([^~]+)~?$')) is not null 
					then  PARSE_DATE('%m/%d/%Y',REGEXP_EXTRACT(ln.line_note, r'~([^~]+)~?$'))  
			else EXTRACT(DATE FROM (PARSE_DATETIME('%m/%d/%Y %H:%M:%S',entry_date_time))) 
		end as order_date
		--,transaction_Date
		--,case when PARSE_DATE('%m/%d/%Y',REGEXP_EXTRACT(ln.line_note, r'~([^~]+)~?$')) is not null 
		--		then  PARSE_DATE('%m/%d/%Y',REGEXP_EXTRACT(ln.line_note, r'~([^~]+)~?$'))  
		--	else inc.transaction_Date end as order_date
		--, 0 as  unit_cogs_amt	
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
			END)                    										AS net_sales_unit_cnt
        ,(ROUND(sum(CASE
						WHEN l.line_Action = 1
							and gross_line_amount >= 0 THEN gross_line_amount-pos_discount_amount
						WHEN l.line_Action = 2
							and gross_line_amount < 0  THEN gross_line_amount-pos_discount_amount
						WHEN l.line_Action = 1 
							and gross_line_amount < 0  THEN (-1)*(gross_line_amount-pos_discount_amount)
						WHEN l.line_Action = 2
							and gross_line_amount >= 0 THEN (-1)*(gross_line_amount-pos_discount_amount)
						ELSE 0
					END)/100, 2))                                             AS net_sales_amt
							
        ,sum(CASE 
				WHEN l.line_Action = 1 
					and gross_line_amount < 0   THEN 1
				WHEN l.line_Action = 2 
					and gross_line_amount >= 0  THEN 1               
				ELSE 0                      
			END)                                            			AS return_sales_unit_cnt
        ,ROUND(sum(CASE        
						WHEN l.line_Action = 1 
							and gross_line_amount < 0  THEN (gross_line_amount-pos_discount_amount) 
						WHEN l.line_Action = 2 
							and gross_line_amount >= 0 THEN (gross_line_amount-pos_discount_amount)
						ELSE      0
						END)/100,2)                                 		AS return_sales_dollars_amt         				
		,ROUND(sum(CASE  WHEN fpnfp_flag = 'F' THEN 
					  (CASE
						  WHEN l.line_Action = 1
					  and gross_line_amount >= 0 THEN gross_line_amount-pos_discount_amount
						  WHEN l.line_Action = 2
					  and gross_line_amount < 0 THEN gross_line_amount-pos_discount_amount
						  WHEN l.line_Action = 1
					  and gross_line_amount < 0 THEN (-1)*(gross_line_amount-pos_discount_amount)
						  WHEN l.line_Action = 2
					  and gross_line_amount >= 0 THEN (-1)*(gross_line_amount-pos_discount_amount)
						  ELSE 0
					  END)
					else 0	  
                    END      )/100,2)                           		AS promotion_net_sales_dollars_amt 
        ,SUM(CASE WHEN fpnfp_flag = 'F' THEN 
                (CASE
                    WHEN l.line_Action = 1
                  and gross_line_amount >= 0 THEN 1
                    WHEN l.line_Action = 2
                  and gross_line_amount < 0 THEN 1
                    WHEN l.line_Action = 1
                  and gross_line_amount < 0 THEN -1
                    WHEN l.line_Action = 2
                  and gross_line_amount >= 0 THEN -1
                    ELSE 0
                    END)
               else 0
				    end)                              						AS promotion_net_sales_unit_cnt		
	   ,ROUND(sum(CASE WHEN  fpnfp_flag <> 'F' THEN
					(CASE WHEN l.line_Action = 1
				  and gross_line_amount >= 0 THEN gross_line_amount-pos_discount_amount
					  WHEN l.line_Action = 2
				  and gross_line_amount < 0 THEN gross_line_amount-pos_discount_amount
					  WHEN l.line_Action = 1
				  and gross_line_amount < 0 THEN (-1)*(gross_line_amount-pos_discount_amount)
					  WHEN l.line_Action = 2
				  and gross_line_amount >= 0 THEN (-1)*(gross_line_amount-pos_discount_amount)
					  ELSE 0
					 END) 
				ELSE 0	 
						 END)/100,2)      								AS markdown_net_sales_dollars_amt 
		,SUM(CASE WHEN  fpnfp_flag <> 'F' THEN 
			  (CASE
				  WHEN l.line_Action = 1
				and gross_line_amount >= 0 THEN 1
				  WHEN l.line_Action = 2
				and gross_line_amount < 0 THEN 1
				  WHEN l.line_Action = 1
				and gross_line_amount < 0 THEN -1
				  WHEN l.line_Action = 2
				and gross_line_amount >= 0 THEN -1
				  ELSE 0
				  END)
			   else 0   
				  end)                        							AS markdown_net_sales_unit_cnt  
	from     
	--edl_landing.ann_ann_aw_transactions_header inc 
(
					select	if_entry_no,store_no,entry_date_time,register_no,transaction_no
						from(
							SELECT	if_entry_no, store_no ,entry_date_time,register_no,transaction_no,
									CASE 
										WHEN interface_control_flag = 30 
								OR interface_control_flag = 10 THEN CONCAT(CAST(store_no AS string),
									':',CAST (register_no AS string),
									':',CAST(EXTRACT (DATE FROM (PARSE_DATETIME('%m/%d/%Y %H:%M:%S',
									entry_date_time))) AS STRING),
									':',
									CAST(transaction_no AS string)) 
										WHEN interface_control_flag IN (10, 20) THEN NULL
										ELSE NULL 
									END AS txn_nbr
							, interface_control_flag
							, rank() OVER (
							PARTITION BY store_no, register_no, transaction_date,
									transaction_no 
							ORDER BY 
									CASE 
										WHEN interface_control_flag = 30 THEN 1
										WHEN interface_control_flag = 20 THEN 2
										ELSE 3 
									END) AS rn
							FROM	edl_landing.ann_ann_aw_transactions_header
						) AS A 
	    WHERE	rn = 1 
		AND txn_nbr IS NOT NULL 
		AND interface_control_flag != 20 ) as inc  --header interface control flag logic
	JOIN edl_landing.ann_ann_aw_transactions_line l
		ON inc.if_entry_no = l.if_entry_no
		AND l.line_object_type = 1
		AND line_void_flag = '0'
	LEFT JOIN edl_landing.ann_ann_aw_transactions_linenotes ln
		ON inc.if_entry_no = ln.if_entry_no
		AND line_note_type = 78  
	LEFT join edl_landing.ann_ann_aw_transactions_merchdtl m
		on m.if_entry_no = l.if_entry_no
		and l.line_id = m.line_id
	LEFT JOIN edl_landing.ann_calendar cal
		--ON transaction_Date = CAST (day_dt AS date)
    ON EXTRACT (DATE FROM (PARSE_DATETIME('%m/%d/%Y %H:%M:%S',entry_date_time)))  = CAST (day_dt AS date)
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
           ON cast(m.sku_id as string) = p.sku
	LEFT JOIN edl_landing.ann_ann_sap_style s
		ON s.style_number = substr(p.article, 10, 6)            
	LEFT JOIN ( SELECT *	FROM 						-- NEED To change after getting load from BI team
						(SELECT *
								,ifnull(full_price_flg,'N') as fpnfp_flag,
								ROW_NUMBER() OVER (PARTITION BY transaction_nbr, item_sku, full_price_flg, batch_id) AS rn
						FROM `p-asna-analytics-002.work.ann_ann_bi_transaction_fpnfp` 
						) a
				WHERE rn = 1 
				)bi
		ON  bi.item_sku = cast(m.sku_id as string)	
		--AND bi.transaction_nbr = CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',CAST(transaction_no AS string))
    AND bi.transaction_nbr =CONCAT(CAST(inc.store_no AS string),':'
                                                                        ,CAST (inc.register_no AS string),':'
                                                                        ,CAST(FORMAT_DATE("%Y%m%d", DATE (PARSE_DATETIME('%m/%d/%Y %H:%M:%S', entry_date_time))) AS STRING) ,':'
                                                                        ,cast(inc.transaction_no AS string))

	where 
		inc.store_no not in (611,612,613,616,617,618,619,626,627) 
		---additional condition
		and inc.store_no not in( 4000,4001,4002,4003,4100,4101,4102,4103,4104,4105,4106,4107,4108)  -- Canada stores not in BI
    and inc.store_no not in (370,371,374,378) -- exclusions from BI
    and p.dept not in ('108','109','155','156','171','180','181','182','183','185','186','187','189','191')
	  and s.style_number  not in ('999099','164562','91482') --dummy styles not in BI
		--additional condition end
				--inc.transaction_series  in ('I','U')  AND -- need to confirm with george on this
		/*and CONCAT(CAST(inc.store_no AS string)	,':'
				,CAST (inc.register_no AS string)	,':'
				,FORMAT_DATE('%Y%m%d',transaction_date), ':'
				,cast(transaction_no AS string)) 
			NOT IN 
					(select CONCAT(CAST(inc_20.store_no AS string),':'
							,CAST (inc_20.register_no AS string),':'
							,FORMAT_DATE('%Y%m%d',inc_20.transaction_date), ':'
							,cast(inc_20.transaction_no AS string)) 
					from edl_landing.ann_ann_aw_transactions_header inc_20  
					where inc_20.interface_control_flag  = 20)  --AND inc_20.transaction_series  in ('I','U') -- need to confirm with george on this */
		 --and  inc.transaction_Date > '2018-08-31'		   -- to avoid overlapping with historical
		 and EXTRACT(DATE FROM (PARSE_DATETIME('%m/%d/%Y %H:%M:%S',entry_date_time))) > '2018-08-31'
	group by 1,2,3,4,5,6,7)a
	 LEFT JOIN   
		(
				 
			SELECT	--sum(abs(Net_Sales_COGS_amt))/sum(abs(net_sales_unit_cnt)) as unit_cogs_amt,
				case when sum(abs(COALESCE(bop_store_inv_cnt, 0))) != 0 then sum(abs(bop_store_inv_actual_amt))/sum(abs(bop_store_inv_cnt))
						--when  sum(abs(bop_store_inv_cnt)) < 0 then (-1)*sum(abs(bop_store_inv_actual_amt))/sum(abs(bop_store_inv_cnt))
					else 0 end as unit_cogs_amt,
					Store_ID,
					Week_ID,
					Style_ID,
					Color_ID,
          sku_id
				FROM	`p-asna-analytics-002.work.inventory_store_sku`         
				where store_id not in (611,612,613,616,617,618,619,626,627) 
				group by 2,3,4,5,6
		) as cogs					
		on  cogs.store_id = a.store_id
	  AND cogs.week_id = a.week_id
		AND cogs.Style_ID = cast(a.style_id as INT64)
		AND cogs.color_id = CAST(a.color_id as INT64)
    AND cogs.sku_id = CAST(a.sku AS INT64)