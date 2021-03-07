CREATE OR REPLACE TABLE analytic_mart.pre_transaction_item_new as
select
	 FARM_FINGERPRINT(concat(replace(h.txn_nbr,':', '|'),'|',cast(i.txn_item_nbr as string))) 		as transaction_item_key
	,FARM_FINGERPRINT(replace(h.txn_nbr,':', '|')) 													as transaction_key	
	,h.txn_nbr 																						as transaction_num
	,i.txn_item_nbr 																				as line_num
	,i.txn_source_cd 																				as transaction_source_cd
	,FARM_FINGERPRINT(cast(i.store_nbr as string)) 													as store_key
	,i.register_nbr 																				as register_num
	,FARM_FINGERPRINT(p.sku)																		as item_key
	--placeholder for class_key
   ,cal.week_id																					as week_id
    ,case 
	  when ret.txn_nbr is null 
		then null
	 else FARM_FINGERPRINT(replace(ret.txn_nbr,':', '|'))
     END 																							as original_transaction_key	
	,EXTRACT(DATE FROM i.txn_dt) 																	as transaction_dt
	,CAST(i.txn_time AS TIME) 																		as transaction_tms
	,i.txn_type_cd 																					as transaction_type_cd
	,i.txn_channel_cd 																				as channel_cd
	,i.item_cogs																					as item_cogs_amt
	,ROUND(i.item_list_price,2) 																	as list_price_amt
	,i.item_sold_qty 																				as item_sold_qty
	,ROUND(i.item_sold_price,2) 																	as item_sold_amt
	,ROUND(i.item_gross_amt,2) 																		as item_gross_amt
	,ROUND(i.item_disc_amt,2) 																		as item_disc_amt
	,ROUND(i.item_net_amt,2) 																		as item_net_amt
	,ret.txn_nbr 																					as item_return_from_transaction_num
	,i.item_ret_cd 																					as item_return_cd
	,EXTRACT(DATE FROM i.item_ret_dt) 																as item_return_dt
	,i.item_ret_qty 																				as item_return_qty
	,ROUND(i.item_ret_amt,2) 																		as item_return_amt
	,case 
		when i.full_price_flg = 'N' 
			THEN 0 
		else 1			
	END 																							as full_price_ind
	,20160101 																						as batch_id
FROM    edl_stage.pre_hst_transaction_item i
JOIN edl_stage.pre_hst_transaction_header h 
		on i.txn_nbr =h.txn_nbr
LEFT JOIN (select 
					txn_nbr
					,txn_id 
 				from edl_stage.pre_hst_transaction_header ret
 				)ret
		on ret.txn_id = i.item_ret_from_txn_id
LEFT JOIN edl_stage.pre_lu_product_vw as p 
 		on i.product_id =p.product_id
LEFT JOIN edl_stage.pre_ann_calendar as cal
 		ON CAST (day_dt AS date) = EXTRACT(date FROM i.txn_dt)
    where EXTRACT(DATE FROM i.txn_dt) between '2016-01-01' and '2019-05-04'
----385691402 --3 min 56 sec elapsed, 53.9 GB processed
    
-- ATG DATA - PURCHASE AND EXCHANGE
UNION DISTINCT
	select     
		FARM_FINGERPRINT(concat(ci.orderid,'|',ci.commerceitemid))			 						as transaction_item_key
		,FARM_FINGERPRINT(ci.orderid) 																as transaction_key 
		,ci.orderid 																				as transaction_num
		,row_number() OVER (PARTITION BY ci.orderid order by ci.commerceitemid) 					as line_num
		,'ATG' 																						as transaction_source_cd                                    
		,FARM_FINGERPRINT(ord.siteid) 																as store_key 
		,1 																							as register_num
    ,FARM_FINGERPRINT(ci.catalogrefid)																			as item_key
     --placeholder for class_key
	 ,cal.week_id																					as week_id
    ,CAST(null AS INT64)                                  as original_transaction_key
		,EXTRACT(DATE FROM ord.submitteddate) 														as transaction_dt 
		,EXTRACT(TIME from ord.submitteddate) 														as transaction_tms
		,CASE when ord.priceinfoamount = 0 then 'EXC' 
			 else 'PUR'		
			END 																					as transaction_type_cd
		,'WEB' 																						as channel_cd
		, cogs.unit_cost_amt                               as item_cogs_amt
		,ci.listprice 																				as list_price_amt
		,ci.commerceitemquantity 																	as item_sold_qty
		,case 
			when ci.saleprice=0 
				then ci.listprice 
			else ci.saleprice 
			end 																					as item_sold_amt
		,case 
			when ci.saleprice=0 
				then ci.listprice*ci.commerceitemquantity
			else ci.saleprice*ci.commerceitemquantity
			end 																					as item_gross_amt
		,ci.itempriceinfoorderdiscountshare 														as item_disc_amt
		,commerceitempriceinfoamount 																as item_net_amt
		,cast (0 as string) 																		as item_return_from_transaction_num
		,cast(null as string) 																		as item_return_cd
		,CAST (NULL as DATE) 																		as item_return_dt
		,0 																							as item_return_qty
		,0 																							as item_return_amt
		,case 
		 when bi.fpnfp_flag = 'N' 
			THEN 0 
		else 1	
		END 																						as full_price_ind
		,ci.batch_id
	FROM  edl_stage.pre_atg_order_header_curr ord
		LEFT JOIN edl_stage.pre_atg_order_commerceItem_curr ci
			on ord.orderid = ci.orderid
			and EXTRACT(DATE FROM ord.submitteddate) > '2019-05-04'
--begin code changes
    LEFT JOIN edl_stage.pre_ann_calendar as cal
		ON CAST (day_dt AS date) = EXTRACT(DATE FROM ord.submitteddate)
      LEFT JOIN  edl_stage.pre_sap_product p
           ON ci.catalogrefid  = p.sku
	  LEFT JOIN edl_stage.pre_sap_style s
		ON s.style_number = substr(p.article, 10, 6) 
		LEFT JOIN (SELECT *	FROM 												
						(SELECT *							
								,ifnull(full_price_flg,'N') as fpnfp_flag					
								,ROW_NUMBER() OVER (PARTITION BY transaction_nbr, item_sku, full_price_flg, batch_id) AS rn					
							FROM edl_stage.pre_ann_ann_bi_transaction_fpnfp							
						) a							
						WHERE rn = 1 									
					)bi									
			ON  bi.transaction_nbr = ci.orderid 											
			and bi.item_sku = ci.catalogrefid 			
inner JOIN   
		(
			select 					
					store_id
					,week_id   --max weekid it has till 201914 , so we are loading atg after that date range
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
    on CAST(cogs.store_id AS STRING) = cast(ord.siteid as string)
		AND cogs.week_id = cal.week_id
	  AND cast(cogs.Style_ID as INT64)  = cast(s.style_number as INT64)
	  AND cast(cogs.color_id as INT64) = CAST(substr(p.color,3,4)  as INT64)	  
	  AND cast(cogs.sku_id as string) = ci.catalogrefid
UNION DISTINCT
-- ATG RETURNS
		select   
			FARM_FINGERPRINT(concat(ret.returnid,'|',ret.commerceitemid))			 				as transaction_item_key		
			,FARM_FINGERPRINT(ret.returnid) 														as transaction_key 
			,upper(ret.returnid)																	as transaction_num
			,row_number() 
					OVER 
					(PARTITION BY ret.returnid
								, ret.commerceitemid 
					order by ret.commerceitemid)													as line_num
			,'ATG' 																					as transaction_source_cd
			,FARM_FINGERPRINT(ord.siteid) 															as store_key 
			,1 																						as register_num
			,FARM_FINGERPRINT(ci.catalogrefid) 																		as item_key
			--placeholder for class_key
			,cal.week_id																					as week_id
			,case 
					when ret.orderid is null 
		      then null
	     else FARM_FINGERPRINT(ret.orderid)
        END 																							as original_transaction_key
			,EXTRACT(DATE FROM (ord.submitteddate)) 												as transaction_dt 
			,EXTRACT(TIME from ord.submitteddate) 													as transaction_tms
			,'RTN' 																					AS transaction_type_cd
			,'WEB' 																					as channel_cd
			, cogs.unit_cost_amt                               as item_cogs_amt
			,NULL 																					as list_price_amt
			,0 																						as item_sold_qty
			,0.0																					as item_sold_amt
			,0.0 																					as item_gross_amt
			,0.0 																					as item_disc_amt
			,0.0 																					as item_net_amt
			,retord.orderid 																		as item_return_from_transaction_num
			,cast(null as string) 																	as item_return_cd
			,EXTRACT( DATE FROM retord.createddate) 												as item_return_dt
			,quantitytoreturn 																		as item_return_qty
			,refundamount 																			as item_return_amt
			,case 
		      when bi.fpnfp_flag = 'N' 
			     THEN 0 
		      else 1 
				END 																				as full_price_ind
			,ret.batch_id																			as batch_id
		from  edl_stage.pre_atg_order_return_commerceitem_curr ret
			LEFT JOIN  edl_stage.pre_atg_order_return_curr retord
				on ret.returnid = retord.returnid
			LEFT JOIN   edl_stage.pre_atg_order_header_curr ord
				on ret.orderid = ord.orderid
			LEFT JOIN edl_stage.pre_atg_order_commerceItem_curr ci
				on ret.orderid = ci.orderid
				and ret.commerceitemid = ci.commerceitemid
    LEFT JOIN edl_stage.pre_ann_calendar as cal
		ON CAST (day_dt AS date) = EXTRACT(DATE FROM ord.submitteddate)
      LEFT JOIN  edl_stage.pre_sap_product p
           ON ci.catalogrefid  = p.sku
	  LEFT JOIN edl_stage.pre_sap_style s
		ON s.style_number = substr(p.article, 10, 6)   
			LEFT JOIN ( SELECT *	FROM 												
								(SELECT *							
										,ifnull(full_price_flg,'N') as fpnfp_flag,					
										ROW_NUMBER() 
										OVER (PARTITION BY transaction_nbr
										, item_sku
										, full_price_flg
										, batch_id) AS rn					
								FROM edl_stage.pre_ann_ann_bi_transaction_fpnfp							
								) a							
						WHERE rn = 1 									
						)bi									
				ON  bi.transaction_nbr = ci.orderid 											
				and bi.item_sku = ci.catalogrefid
inner JOIN   
		(
			select 					
					store_id
					,week_id   --max weekid it has till 201914 , so we are loading atg after that date range
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
    on CAST(cogs.store_id AS STRING) = cast(ord.siteid as string)
		AND cogs.week_id = cal.week_id
	  AND cast(cogs.Style_ID as INT64)  = cast(s.style_number as INT64)
	  AND cast(cogs.color_id as INT64) = CAST(substr(p.color,3,4)  as INT64)	  
	  AND cast(cogs.sku_id as string) = ci.catalogrefid        
    where EXTRACT(DATE FROM ord.submitteddate) > '2019-05-04'
union distinct
select	
	FARM_FINGERPRINT(concat(CAST(h.store_no AS string)
	,'|',CAST (h.register_no AS string)
	,'|',CAST(FORMAT_TIMESTAMP("%Y%m%d", h.entry_date_time) AS STRING)
	,'|',cast(h.transaction_no AS string)
	,'|',cast(l.line_id as string)
					  )) 																			as transaction_item_key,
	FARM_FINGERPRINT(concat(CAST(h.store_no AS string)
	,'|',CAST (h.register_no AS string)
	,'|',CAST(FORMAT_TIMESTAMP("%Y%m%d", h.entry_date_time) AS STRING)
	,'|',cast(h.transaction_no AS string)
					  )) 																			as transaction_key,
	CONCAT(CAST(h.store_no AS string)
	,':',CAST (h.register_no AS string)
	,':',CAST(FORMAT_TIMESTAMP("%Y%m%d", h.entry_date_time) AS STRING)
	,':',cast(h.transaction_no AS string)
					  ) 																			as transaction_num
	,l.line_id 																						as line_num
	,'AW' 																							as transaction_source_cd
	,FARM_FINGERPRINT(cast(store_no as string)) 													as store_key
	,register_no 																					as register_num
  ,FARM_FINGERPRINT(m.sku_id )																		as item_key
	--placeholder for class_key
	 ,cal.week_id																				as week_id
   ,case 
	  when return_from_transaction_no is NULL
    or return_from_store IS NULL
    or return_from_register IS NULL
    or return_from_date IS NULL
		then null
	  else   FARM_FINGERPRINT(CONCAT(CAST(r.return_from_store as string)
  ,':',cast(r.return_from_register as string)
  ,':',cast(FORMAT_DATE("%Y%m%d", r.return_from_date) as string)
  ,':',cast(r.return_from_transaction_no as string)))
     END 																							as original_transaction_key	
	,EXTRACT(DATE FROM entry_date_time) 															as transaction_dt
	,EXTRACT(TIME FROM entry_date_time) 															as transaction_tms
			,case when h.tender_total > 0 
					then 'PUR'
				when h.tender_total = 0 
					then 'EXC'
				when h.tender_total < 0
					then 'RTN'
				else NULL
			END 																					as transaction_type_cd		
	,'RET' 																							as channel_cd
	, cogs.unit_cost_amt                               as item_cogs_amt
	,round(ticket_price,2) 																			as list_price_amt
	,CASE 
		WHEN l.line_Action = 1 
				and gross_line_amount >= 0 
			THEN 1
		WHEN l.line_Action = 24 
			and gross_line_amount >= 0 
			THEN 1
		WHEN l.line_Action = 2 
			and gross_line_amount < 0 
			THEN 1
		ELSE 0 
		END 																						as item_sold_qty
	,CASE 
			WHEN l.line_Action = 1 
					and gross_line_amount >= 0 
				THEN ROUND(gross_line_amount,2)
			WHEN l.line_Action = 24 
					and gross_line_amount >= 0 
				THEN ROUND(gross_line_amount,2)
			WHEN l.line_Action = 2 
					and gross_line_amount < 0
				THEN ROUND(gross_line_amount,2)
			ELSE 0 
		END 																						as item_sold_amt	
	,CASE 
			WHEN l.line_Action = 1 
					and gross_line_amount >= 0 
				THEN ROUND(gross_line_amount,2)
			WHEN l.line_Action = 2 
					and gross_line_amount < 0 
				THEN ROUND(gross_line_amount,2)
			ELSE 0 
		END 																						as item_gross_amt
	,CASE 								
			WHEN l.line_Action = 1 								
					and gross_line_amount >= 0 								
				THEN ROUND(pos_discount_amount,2)								
			WHEN l.line_Action = 2 								
					and gross_line_amount < 0 								
				THEN ROUND(pos_discount_amount,2)								
			ELSE 0 								
			END 																					as item_disc_amt	
	,CASE 								
			WHEN l.line_Action = 1 								
					and gross_line_amount >= 0 								
				THEN ROUND(gross_line_amount-pos_discount_amount,2)								
			WHEN l.line_Action = 2 								
					and gross_line_amount < 0 								
				THEN ROUND(gross_line_amount-pos_discount_amount,2)								
			ELSE 0 								
		END 																						as item_net_amt	
	,cast (r.return_from_transaction_no as string)											as item_return_from_transaction_num
	,cast(r.return_reason_code as string) 															as item_return_cd
	,r.return_from_date 																			as item_return_dt
	,CASE  
			WHEN l.line_Action = 1  
					and gross_line_amount < 0   
				THEN 1 
			WHEN l.line_Action = 2  
					and gross_line_amount >= 0  
				THEN 1
			ELSE 0 
		END 																						as item_return_qty
	,CASE         
			WHEN l.line_Action = 1  
					and gross_line_amount < 0  
				THEN round(gross_line_amount-pos_discount_amount,2)  
			WHEN l.line_Action = 2  
					and gross_line_amount >= 0 
				THEN round(gross_line_amount-pos_discount_amount,2)
			ELSE 0 
		END 																						as item_return_amt
  			,case 
		      when bi.fpnfp_flag = 'N' 
			     THEN 0 
		      else 1 
				END 																				as full_price_ind
	,l.batch_id 																					as batch_id
	FROM	`edl_stage.pre_aw_transaction_line`  as l
	INNER JOIN (
	select	if_entry_no,
			store_no,
			register_no,
			transaction_no,
			entry_date_time,
			tender_total
	from (
			select	if_entry_no,
					store_no,
					register_no,
					transaction_no,
					entry_date_time,
					tender_total,
					row_number() over(
					partition by CONCAT(CAST(store_no AS string)
										,'|',CAST (register_no AS string)
										,'|',CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING)
										,'|',cast(transaction_no AS string)) 
								order by if_entry_no desc) as rn
			from edl_stage.pre_aw_transaction_header
			where store_no not in (611,612,613,616,617)		 
			and EXTRACT(DATE FROM entry_date_time)>'2019-05-04'
			and CONCAT(CAST(store_no AS string)
				   ,'|',CAST (register_no AS string)
				   ,'|',CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING)
				   ,'|',CAST(transaction_no AS string))
						  NOT IN (select CONCAT(CAST(tmp.store_no AS string)
												,'|',CAST (tmp.register_no AS string)
												,'|',CAST(FORMAT_TIMESTAMP("%Y%m%d",tmp.entry_date_time) AS STRING)
												,'|',cast(tmp.transaction_no AS string)) 
								  from	edl_stage.pre_aw_transaction_header tmp  
								  where	tmp.interface_control_flag  = 20 )
		)as a 
	where rn=1
	) as h 
		ON l.if_entry_no = h.if_entry_no
		AND (l.line_object_type =1 or (line_object=618 and line_action=24))
		AND l.line_void_flag = '0'
	LEFT OUTER JOIN `edl_stage.pre_aw_transaction_merch_detail`  as m 
		ON (l.if_entry_no = m.if_entry_no 
		AND l.line_id = m.line_id)
	LEFT OUTER JOIN edl_landing.ann_ann_aw_transactions_returndtl as r 
		ON (l.if_entry_no = r.if_entry_no 
		AND l.line_id = r.line_id) 
  -- begin adding new code to get week_id style_key  
  LEFT JOIN edl_stage.pre_ann_calendar cal		
      ON EXTRACT(DATE FROM h.entry_date_time) = CAST (day_dt AS date)
  LEFT JOIN  edl_stage.pre_sap_product p
           ON m.sku_id  = p.sku
	LEFT JOIN edl_stage.pre_sap_style s
		ON s.style_number = substr(p.article, 10, 6) 
  -- end adding new code to get week_id style_key  
  LEFT JOIN ( SELECT *	FROM 												
								(SELECT *							
										,ifnull(full_price_flg,'N') as fpnfp_flag,					
										ROW_NUMBER() 
										OVER (PARTITION BY transaction_nbr
										, item_sku
										, full_price_flg
										, batch_id) AS rn					
								FROM edl_stage.pre_ann_ann_bi_transaction_fpnfp		
                								) a							
						WHERE rn = 1 									
						)bi									
				ON  bi.transaction_nbr = 	CONCAT(CAST(h.store_no AS string)	,':',CAST (h.register_no AS string)	,':',CAST(FORMAT_TIMESTAMP("%Y%m%d", h.entry_date_time) AS STRING)	,':',cast(h.transaction_no AS string) )  											  and bi.item_sku = m.sku_id
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
    on CAST(cogs.store_id AS STRING) = cast(h.store_no as string)
		AND cogs.week_id = cal.week_id
		AND cast(cogs.Style_ID as INT64)  = cast(s.style_number as INT64)
		AND cast(cogs.color_id as INT64) = CAST(substr(p.color,3,4)  as INT64)	  
		AND cast(cogs.sku_id as string) = m.sku_id