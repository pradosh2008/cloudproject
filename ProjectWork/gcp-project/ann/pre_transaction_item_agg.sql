--FARM_FINGERPRINT(replace(h.txn_nbr,':', '|')) as transaction_key
CREATE OR REPLACE TABLE analytic_mart.pre_transaction_item_agg AS 
select a.*,ifnull(round(unit_cost_amt,2),0) unit_cogs_amt    
from(SELECT 
        CAST(hist.store_nbr AS STRING) AS store_id
        , fiscal_week_id AS week_id
        , CAST(pr.style_nbr AS STRING) AS style_id
		, FARM_FINGERPRINT(CAST(pr.style_nbr AS STRING)) AS style_key
        , CAST( pr.color_cd as string) AS color_id
        , pr.sku as sku_id
		, FARM_FINGERPRINT(pr.sku) as item_key
  --      , EXTRACT (DATE FROM hist.txn_dt) AS transaction_date
        , EXTRACT (DATE FROM hist.txn_dt) AS order_date
        --, sum(item_cogs) as unit_cogs_amt
        ,SUM( CASE 
					WHEN item_sold_qty>0  THEN item_sold_qty 
					ELSE -item_ret_qty 
			END) 												as  net_sales_unit_cnt
        ,ROUND(SUM( CASE 
					WHEN item_sold_qty>0 THEN item_net_amt 
					ELSE -item_ret_amt 
			END),2)							 					as  net_sales_amt
        ,SUM( CASE 
					WHEN item_ret_qty>0 THEN item_ret_qty 
					ELSE -item_ret_qty 
			END)							 					as  return_sales_unit_cnt
        ,ROUND(SUM( CASE 
					WHEN item_ret_qty>0 THEN item_ret_amt 
					ELSE 0 
			END),2)				 								as  return_sales_dollars_amt	
        ,SUM( CASE 
					WHEN t.full_price_flg <> 'F' 
				and item_sold_qty > 0  THEN item_sold_qty 
					WHEN t.full_price_flg <> 'F' 
				and item_ret_qty > 0  THEN -item_ret_qty 
				ELSE 0 
			END)  												as markdown_net_sales_unit_cnt
        ,ROUND(SUM( CASE 
						WHEN t.full_price_flg <> 'F' 
					and item_sold_qty > 0  THEN item_net_amt
						WHEN t.full_price_flg <> 'F' 
					and item_ret_qty > 0  THEN -item_ret_amt 
						ELSE 0 
					END),2)  											as markdown_net_sales_dollars_amt      		
        ,SUM( CASE 
					WHEN item_ret_qty>0 
				and full_price_flg='F' THEN -item_ret_qty
					WHEN item_sold_qty>0 
				and full_price_flg='F' THEN item_sold_qty
					ELSE 0 
				END) 												as promotion_net_sales_unit_cnt
        ,ROUND(SUM( CASE 
						WHEN item_ret_qty>0 
					and full_price_flg='F' THEN -round(item_ret_amt,2)
						WHEN item_sold_qty>0 
					and full_price_flg='F' THEN round(item_net_amt,2)
						ELSE 0 
				END),2) 											as promotion_net_sales_dollars_amt      
    FROM edl_stage.pre_hst_transaction_header hist
              JOIN edl_stage.pre_hst_transaction_item t
          ON hist.txn_nbr = t.txn_nbr
              LEFT JOIN edl_stage.pre_ann_calendar cal
          ON CAST (day_dt AS date) = EXTRACT(date FROM hist.txn_dt)
              LEFT JOIN edl_stage.pre_lu_product_vw pr
          ON pr.product_id = t.product_id
--              LEFT JOIN edl_landing.lu_style_vw s
--          ON s.style_nbr = pr.style_nbr                
   -- WHERE hist.txn_source_cd ='AW'
    where EXTRACT (DATE from hist.txn_dt) between '2016-01-01' and  '2019-05-04' 
    GROUP BY 1,2,3,4,5,6,7,8
UNION DISTINCT 
			select --'Incremental' as source
		cast (inc.store_no as STRING)  as store_id
		,cal.fiscal_week_id as week_id
		,s.style_number style_id
		,FARM_FINGERPRINT(s.style_number) style_key
		,substr(p.color,3,4) color_id  
		,p.sku as  sku_id
		,FARM_FINGERPRINT(p.sku) item_key
		,case 
			when PARSE_DATE('%m/%d/%Y',REGEXP_EXTRACT(ln.line_note, r'~([^~]+)~?$')) is not null 
					then  PARSE_DATE('%m/%d/%Y',REGEXP_EXTRACT(ln.line_note, r'~([^~]+)~?$'))  
			else EXTRACT(DATE FROM (entry_date_time)) 
		end as order_date
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
					END), 2))                                             AS net_sales_amt
							
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
						END),2)                                 		AS return_sales_dollars_amt         				
--		,ROUND(sum(CASE  WHEN fpnfp_flag = 'F' THEN 
		,ROUND(sum(CASE  WHEN fpnfp_flag = 'F' and pos_discount_amount != 0  THEN 
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
                    END      ),2)                           		AS promotion_net_sales_dollars_amt 
--        ,SUM(CASE WHEN fpnfp_flag = 'F'  THEN 
        ,SUM(CASE WHEN fpnfp_flag = 'F'  and pos_discount_amount != 0 THEN 
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
						 END),2)      								AS markdown_net_sales_dollars_amt 
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
	edl_stage.pre_aw_transaction_header inc 
	JOIN edl_stage.pre_aw_transaction_line l
		ON inc.if_entry_no = l.if_entry_no
		AND l.line_object_type = 1
		AND line_void_flag = '0'
	LEFT JOIN edl_stage.pre_aw_transaction_linenotes ln
		ON inc.if_entry_no = ln.if_entry_no
		AND line_note_type = 78  
	LEFT join edl_stage.pre_aw_transaction_merch_detail m
		on m.if_entry_no = l.if_entry_no
		and l.line_id = m.line_id
	LEFT JOIN edl_stage.pre_ann_calendar cal 
      ON EXTRACT (DATE FROM (entry_date_time))  = CAST (day_dt AS date)
	LEFT JOIN  edl_stage.pre_sap_product p        
           ON cast(m.sku_id as string) = p.sku
	LEFT JOIN edl_stage.pre_sap_style s
		ON s.style_number = substr(p.article, 10, 6)            
	LEFT JOIN ( SELECT *	FROM 					
						(SELECT *
								,ifnull(full_price_flg,'N') as fpnfp_flag,
								ROW_NUMBER() OVER (PARTITION BY transaction_nbr, item_sku, full_price_flg, batch_id) AS rn
					       FROM edl_stage.pre_ann_ann_bi_transaction_fpnfp
						) a
				WHERE rn = 1 
				)bi
		ON  bi.item_sku = cast(m.sku_id as string)			
    AND bi.transaction_nbr =CONCAT(CAST(inc.store_no AS string),':'
                                                                        ,CAST (inc.register_no AS string),':'                                                
																	                                     ,CAST(FORMAT_DATE("%Y%m%d", DATE ( entry_date_time)) AS STRING) ,':'
                                                                        ,cast(inc.transaction_no AS string))

	where 
		inc.store_no not in (611,612,613,616,617,618,619,626,627) 
    and inc.store_no not in (370,371,374,378) -- exclusions from BI
    and p.dept not in ('108','109','155','156','171','180','181','182','183','185','186','187','189','191')-- exclusions from BI
		 and EXTRACT(DATE FROM (entry_date_time)) > '2019-05-04' 
	group by 1,2,3,4,5,6,7,8
UNION DISTINCT 
	select 
      case when atg.siteid = '613' then '612' else  atg.siteid end as store_id
      ,cal.fiscal_week_id as week_id
	   ,s.style_number style_id
	   ,FARM_FINGERPRINT(s.style_number) style_key
	   ,substr(p.color,3,4) color_id
      ,atg.sku  as sku_id
	   ,FARM_FINGERPRINT(atg.sku) as item_key
      ,atg.order_date
      ,sum(net_sales_unit_cnt) as net_sales_unit_cnt
      ,round(sum(net_sales_amt),2)  as net_sales_amt
      ,sum(return_sales_unit_cnt) as return_sales_unit_cnt
      ,round(sum(return_sales_dollars_amt),2) as return_sales_dollars_amt
      ,sum(markdown_net_sales_unit_cnt) markdown_net_sales_unit_cnt
      ,round(sum(markdown_net_sales_dollars_amt),2) markdown_net_sales_dollars_amt 
      ,sum(promotion_net_sales_unit_cnt) promotion_net_sales_unit_cnt
      ,round(sum(promotion_net_sales_dollars_amt),2)  promotion_net_sales_dollars_amt
from 
      (select 
          ord.orderid 
          ,ci.catalogrefid as sku 
          ,ord.siteid 
          ,EXTRACT(DATE FROM (ord.submitteddate)) as order_date
          ,sum(ci.commerceitemquantity) as net_sales_unit_cnt
          ,sum(ci.commerceitempriceinfoamount) as net_sales_amt   
          ,0 as return_sales_unit_cnt
          ,0 as return_sales_dollars_amt
		      , sum(case when bi.fpnfp_flag != 'F' then ci.commerceitemquantity else 0 end) as markdown_net_sales_unit_cnt
		      , sum(case when bi.fpnfp_flag != 'F' then ci.commerceitempriceinfoamount else 0 end) as markdown_net_sales_dollars_amt	  
		      --, sum(case when bi.fpnfp_flag = 'F' and ifnull(ca.totaladjustment_ItemDiscount,0) <> 0 then ci.commerceitemquantity else 0 end) as promotion_net_sales_unit_cnt
          --, sum(case when fpnfp_flag = 'F' and ifnull(ca.totaladjustment_ItemDiscount,0) <> 0 then ci.commerceitempriceinfoamount else 0 end) as promotion_net_sales_dollars_amt	       
          , sum(case when bi.fpnfp_flag = 'F' and ifnull(cc.commerceitemcouponsdiscountamt, 0) <> 0 then ci.commerceitemquantity else 0 end) as promotion_net_sales_unit_cnt
          , sum(case when fpnfp_flag = 'F' and ifnull(cc.commerceitemcouponsdiscountamt, 0) <> 0 then ci.commerceitempriceinfoamount else 0 end) as promotion_net_sales_dollars_amt	       
      FROM edl_stage.pre_atg_order_header_curr ord
      join edl_stage.pre_atg_order_commerceItem_curr ci
        on ord.orderid = ci.orderid
      join edl_stage.pre_atg_order_commerceitemadjustments_curr ca
        on ca.orderid = ord.orderid
        and ca.commerceitemid  = ci.commerceitemid 
      join edl_stage.pre_atg_order_commerceitemcoupons_curr  cc
        on cc.orderid = ca.orderid
        and ca.commerceitemid  = cc.commerceitemid
	  LEFT JOIN ( SELECT *	FROM 												
								(SELECT *							
										,ifnull(full_price_flg,'N') as fpnfp_flag,					
										ROW_NUMBER() OVER (PARTITION BY transaction_nbr, item_sku, full_price_flg, batch_id) AS rn					
								FROM edl_stage.pre_ann_ann_bi_transaction_fpnfp							
								) a							
						WHERE rn = 1 									
						)bi									
				ON  bi.transaction_nbr = ci.orderid 											
			and bi.item_sku = ci.catalogrefid 		
      group by 1,2,3,4
      union distinct
      select  
			ret.returnid 
		  ,it.catalogrefid as sku
		  ,ord.siteid
		  ,EXTRACT( DATE FROM ret.createddate) as order_date 
		  , sum(-1*retc.quantitytoreturn)   as net_sales_unit_cnt
		  , sum(-1* retc.refundamount) as net_sales_amt
		  , sum(retc.quantitytoreturn) as return_sales_unit_cnt
		  , sum(retc.refundamount) as return_sales_dollars_amt
		  , sum(case when bi.fpnfp_flag != 'F' then retc.quantitytoreturn else 0 end) as markdown_net_sales_unit_cnt
		  , sum(case when bi.fpnfp_flag != 'F' then retc.refundamount else 0 end) as markdown_net_sales_dollars_amt	  
		  --, sum(case when bi.fpnfp_flag = 'F' and ifnull(ca.totaladjustment_ItemDiscount,0) <> 0 then it.commerceitemquantity else 0 end) as promotion_net_sales_unit_cnt
      --, sum(case when fpnfp_flag = 'F' and ifnull(ca.totaladjustment_ItemDiscount,0) <> 0 then it.commerceitempriceinfoamount else 0 end) as promotion_net_sales_dollars_amt	 
      , sum(case when bi.fpnfp_flag = 'F' and ifnull(cc.commerceitemcouponsdiscountamt, 0) <> 0 then retc.quantitytoreturn else 0 end) as promotion_net_sales_unit_cnt
      , sum(case when fpnfp_flag = 'F' and ifnull(cc.commerceitemcouponsdiscountamt, 0) <> 0 then retc.refundamount else 0 end) as promotion_net_sales_dollars_amt	 
      from edl_stage.pre_atg_order_return_curr ret
      join edl_stage.pre_atg_order_return_commerceitem_curr retc
        on retc.returnid = ret.returnid
        and retc.orderid = ret.orderid
      join edl_stage.pre_atg_order_commerceItem_curr it
        on retc.orderid = it.orderid
        and retc.commerceitemid = it.commerceitemid
      join   edl_stage.pre_atg_order_header_curr ord
       on ret.orderid = ord.orderid      
      join edl_stage.pre_atg_order_commerceitemcoupons_curr  cc
        on cc.orderid = retc.orderid
        and cc.commerceitemid = retc.commerceitemid  
	  LEFT JOIN ( SELECT *	FROM 												
								(SELECT *							
										,ifnull(full_price_flg,'N') as fpnfp_flag,					
										ROW_NUMBER() OVER (PARTITION BY transaction_nbr, item_sku, full_price_flg, batch_id) AS rn					
								FROM edl_stage.pre_ann_ann_bi_transaction_fpnfp							
								) a							
						WHERE rn = 1 									
						)bi									
				ON  bi.transaction_nbr = it.orderid 											
			and bi.item_sku = it.catalogrefid 		
      group by 1,2,3,4) atg
  LEFT JOIN edl_stage.pre_ann_calendar cal		
      ON atg.order_date  = CAST (day_dt AS date)
	LEFT JOIN  edl_stage.pre_sap_product p
           ON atg.sku  = p.sku
	LEFT JOIN edl_stage.pre_sap_style s
		ON s.style_number = substr(p.article, 10, 6)     
where atg.order_date >'2019-05-04' 		
group by 1,2,3,4,5,6,7,8
	)a
LEFT JOIN   
		(
			select 					
					store_id
					,week_id
					,style_id
					,color_id		
					,sku_id
					,max(cogs_amt) unit_cost_amt
				from(        					
					select s.*
							 ,case when bop_store_inv_cnt <> 0 
										then SAFE_DIVIDE(bop_store_inv_actual_amt,bop_store_inv_cnt)
							-- This will give the cogs amount from previous week if current week is 0 
									when LAG (ROUND(SAFE_DIVIDE(bop_store_inv_actual_amt,bop_store_inv_cnt),2))  
														over (partition by s.chain_id,s.store_id,s.style_id,s.color_id,s.size_id, s.sku_id order by s.week_id )  is not null							
										 then LAG (ROUND(SAFE_DIVIDE(bop_store_inv_actual_amt,bop_store_inv_cnt),2)) 
														over (partition by s.chain_id,s.store_id,s.style_id,s.color_id,s.size_id, s.sku_id order by s.week_id )    
							-- If previous week is also zero then get the max amount															
								   else max_amt           
							  end as cogs_amt  
							from 
								(select chain_id
										,store_id
										,week_id
										,style_id
										,color_id
										,size_id
										,sku_id
										,bop_store_inv_actual_amt
										,bop_store_inv_cnt
										,ROW_NUMBER() OVER (PARTITION BY chain_id,store_id,week_id,style_id,color_id,size_id,sku_id ORDER BY abs(bop_store_inv_cnt) DESC ) rn -- to remove dups from table                   
								FROM  demand_forecast.inventory_store_sku s    
								)s 
							JOIN -- to get MAX cogs amt at store,sky level
								(
									select chain_id
										,store_id
										,style_id
										,color_id
										,size_id
										,sku_id
										,max(safe_divide(bop_store_inv_actual_amt,bop_store_inv_cnt) ) max_amt
									from  demand_forecast.inventory_store_sku 
									group by chain_id
										,store_id
										,style_id
										,color_id
										,size_id
										,sku_id
								) mx
								on mx.chain_id = s.chain_id 
								and mx.store_id = s.store_id 
								and	mx.style_id = s.style_id 
								and	mx.color_id = s.color_id 
								and	mx.size_id= s.size_id 
								and	mx.sku_id = s.sku_id
							where rn = 1
			)unit_amt        
			group by 1,2,3,4,5
		) as cogs					
		on  CAST(cogs.store_id AS STRING) = a.store_id
		AND cogs.week_id = a.week_id
		AND cast(cogs.Style_ID as INT64)  = cast(a.style_id as INT64)
		AND cast(cogs.color_id as INT64) = CAST(a.color_id as INT64)	  
		AND cast(cogs.sku_id as string) = a.sku_id 