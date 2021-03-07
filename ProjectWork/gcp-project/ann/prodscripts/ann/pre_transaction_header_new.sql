CREATE OR REPLACE table analytic_mart.pre_transaction_header_new as
SELECT 
h.transaction_key
, h.transaction_num
, h.transaction_source_cd
, h.transaction_status_cd
, h.store_key
, cal.fiscal_week_id as week_id
, h.register_num
, h.transaction_dt
, h.transaction_tms
, h.transaction_type_cd
, h.channel_cd
, h.transaction_net_amt
, h.transaction_tax_amt
, h.transaction_net_ret_amt
, h.batch_id
FROM (
select 
	 FARM_FINGERPRINT(txn_nbr) 								as transaction_key
	,TXN_NBR 												as transaction_num
	,TXN_SOURCE_CD 																as transaction_source_cd
	,TXN_TYPE_CD																as transaction_type_cd 
	,TXN_STATUS_CD 																as transaction_status_cd
	,FARM_FINGERPRINT(cast(STORE_NBR as string)) 								as store_key
	,REGISTER_NBR 																as register_num
	,EXTRACT(DATE FROM TXN_DT) 													as transaction_dt
	,EXTRACT(TIME FROM TXN_DT) 													as transaction_tms
	,TXN_CHANNEL_CD 															as channel_cd 
	,round(TXN_NET_AMT,2)														as transaction_net_amt
  ,round(TXN_TAX_AMT,2)                             							as transaction_tax_amt
	,round(txn_ret_net_amt,2)               									as transaction_net_ret_amt
	,20160101 																	as batch_id
from edl_stage.pre_hst_transaction_header H

  ---
  where EXTRACT(DATE FROM txn_dt)  between '2016-01-01' and '2019-05-04'
UNION DISTINCT
--ATG 
select FARM_FINGERPRINT(ord.orderid) 											as transaction_key
		,ord.orderid 															as transaction_num
		,'ATG' 																	as transaction_source_cd
		,CASE when ord.priceinfoamount = 0 then 'EXC' 
			 else 'PUR'		
			END 																as transaction_type_cd
		,CASE TRIM(UPPER(ord.orderstate)) 
                        WHEN 'COMPLETE' 				THEN 'C'
                        WHEN 'FAILED' 					THEN 'F'
                        WHEN 'SUBMITTED' 				THEN 'S'
                        WHEN 'INCOMPLETE' 				THEN 'I'
                        WHEN 'HOLD' 					THEN 'H'
                        WHEN 'PROCESSING' 				THEN 'P'
                        WHEN 'READY_FOR_PICKUP' 		THEN 'READY_FOR_PICKUP'
                        WHEN 'NO_PENDING_ACTION'		THEN 'NPA'
                        WHEN 'REMOVED' 					THEN 'R'
                        WHEN 'SHIPPED' 					THEN 'SH'
                        WHEN 'PENDING_MERCHANT_ACTION' 	THEN 'PMA'
                        WHEN 'PENDING_CUSTOMER_RETURN' 	THEN 'PMA'
                        ELSE 'UNK' 
			END 																as transaction_status_cd
		,FARM_FINGERPRINT(cast(ord.siteid as string)) 							as store_key
		,1 																		as register_num
--		,s.brand_cd as brand_cd
		,EXTRACT(DATE FROM (ord.submitteddate)) 								as transaction_dt
		,EXTRACT(TIME FROM ord.submitteddate) 									as transaction_tms
		,'WEB' 																	as channel_cd
		,ord.priceinfoamount   													as transaction_net_amt
		,ord.orderpriceinfotax 													as transaction_tax_amt
		,0 																		as transaction_net_ret_amt	
		,ord.batch_id 															as batch_id
from edl_stage.pre_atg_order_header_curr ord
  WHERE EXTRACT(DATE FROM (submitteddate)) > '2019-05-04' 
UNION DISTINCT
select FARM_FINGERPRINT(ret.returnid) 											as transaction_key
		,UPPER(ret.returnid) 													as transaction_num
		,'ATG' 																	as transaction_source_cd
		,'RTN' 																	AS transaction_type_cd
		,CASE TRIM(UPPER(orderstate)) 
                        WHEN 'COMPLETE' THEN 'C'
                        WHEN 'FAILED' THEN 'F'
                        WHEN 'SUBMITTED' THEN 'S'
                        WHEN 'INCOMPLETE' THEN 'I'
                        WHEN 'HOLD' THEN 'H'
                        WHEN 'PROCESSING' THEN 'P'
                        WHEN 'READY_FOR_PICKUP' THEN 'READY_FOR_PICKUP'
                        WHEN 'NO_PENDING_ACTION' THEN 'NPA'
                        WHEN 'REMOVED' THEN 'R'
                        WHEN 'SHIPPED' THEN 'SH'
                        WHEN 'PENDING_MERCHANT_ACTION' THEN 'PMA'
                        WHEN 'PENDING_CUSTOMER_RETURN' THEN 'PMA'
                        ELSE 'UNK' END 											as transaction_status_cd
		,FARM_FINGERPRINT(cast(siteid as string)) 								as store_key
		,1 																		as register_num
--		,s.brand_cd as brand_cd
		,EXTRACT(DATE FROM (ret.createddate)) 									as transaction_dt
		,EXTRACT(TIME FROM ret.createddate)										as transaction_tms
		,'WEB' 																	as channel_cd
		, 0                                             						as transaction_net_amt 	
		,ret.transaction_tax_amt 												as transaction_tax_amt
    ,ret.transaction_net_amt 													as transaction_net_ret_amt
--		,NULL 																	as tender_total_amt
		,ret.batch_id 															as batch_id
from   (select
 retord.orderid,
 retord.returnid,
 retord.createddate,
 ord.orderstate,
 ord.siteid,
 sum(ret.refundamount) as transaction_net_amt,
 retord.actualtaxrefund as transaction_tax_amt,
 retord.batch_id  
from  edl_stage.pre_atg_order_return_curr retord
	LEFT JOIN  edl_stage.pre_atg_order_return_commerceitem_curr ret
	on ret.returnid = retord.returnid
	LEFT JOIN   edl_stage.pre_atg_order_header_curr ord
	on ret.orderid = ord.orderid
--	LEFT JOIN edl_stage.pre_atg_order_commerceItem_curr ci
--	on ret.orderid = ci.orderid
--	and ret.commerceitemid = ci.commerceitemid
  group by 1,2,3,4,5,7,8
) as ret
  WHERE EXTRACT(DATE FROM ret.createddate) > '2019-05-04' 
union distinct
select
	transaction_key
	,transaction_num
	,transaction_source_cd
	,transaction_type_cd
	,transaction_status_cd 
	,store_key
	,register_num
	--,s.brand_cd                 as brand_cd
	,transaction_dt
	,transaction_tms
	,channel_cd
	,case when transaction_remark <> 'USD' 
          then aw_item_net_amt*cast(ukurs_curr as float64)
          else aw_item_net_amt 
    end      																	as transaction_net_amt
	,case when transaction_remark <> 'USD' 
          then aw_item_tax_amt*cast(ukurs_curr as float64)
          else aw_item_tax_amt
     end     																	as transaction_tax_amt
	,case when transaction_remark <> 'USD' 
          then aw_item_ret_amt*cast(ukurs_curr as float64)
          else aw_item_ret_amt
    end      																	as transaction_net_ret_amt	
	,batch_id
	from
	(
	select
		FARM_FINGERPRINT(concat(CAST(store_no AS string)
		,':',CAST (register_no AS string)
		,':',CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING)
		,':',cast(transaction_no AS string)))         							as transaction_key
		,CONCAT(CAST(store_no AS string)
		,':',CAST (register_no AS string)
		,':',CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING)
		,':',cast(transaction_no AS string))         							as transaction_num
		,'AW'                  													as transaction_source_cd
		,case when h.tender_total > 0 
					then 'PUR'
				when h.tender_total = 0 
					then 'EXC'
				when h.tender_total < 0
					then 'RTN'
				else NULL
			END 																as transaction_type_cd
		,'C'                  													as transaction_status_cd
		,FARM_FINGERPRINT(cast(store_no as string))        						as store_key
		--  ,store_no                
		,register_no                											as register_num
		,entry_date_time
		,EXTRACT(DATE FROM entry_date_time)          							as transaction_dt
		,EXTRACT(TIME FROM entry_date_time)          							as transaction_tms
		,'RET'                  												as channel_cd
--		,tender_total                											as tender_total_amt
		,batch_id 																as batch_id
		,row_number() 
				over(partition by 
						CONCAT(CAST(store_no AS string)
						,':',CAST (register_no AS string)
						,':',CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING)
						,':',cast(transaction_no AS string)) 
					order by if_entry_no desc)           as rn
		,h.if_entry_no
    ,h.transaction_remark
	from edl_stage.pre_aw_transaction_header h
		where store_no not in (611,612,613,616,617)
		and EXTRACT(DATE FROM entry_date_time) > '2019-05-04' 
		and 
			CONCAT(CAST(store_no AS string),':'
			,CAST (register_no AS string),':'
			,CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING),':'
			,CAST(transaction_no AS string))
				NOT IN 
					(select CONCAT(CAST(tmp.store_no AS string),':'
					,CAST (tmp.register_no AS string),':'
					,CAST(FORMAT_TIMESTAMP("%Y%m%d",tmp.entry_date_time) AS STRING),':'
					,cast(tmp.transaction_no AS string)) 
					from edl_stage.pre_aw_transaction_header tmp  
					where tmp.interface_control_flag  = 20)
		) as h 
		left join 
			(
				select 
					l.if_entry_no 
					,round(sum(CASE
								   WHEN l.line_Action = 1 
									and  l.line_object_type = 1  
									AND line_void_flag = '0'
									and gross_line_amount >= 0 
								  THEN gross_line_amount-pos_discount_amount      
								   WHEN l.line_Action = 1 
									and  l.line_object_type = 1  
									AND line_void_flag = '0'
									and gross_line_amount < 0  
								  THEN (-1)*(gross_line_amount-pos_discount_amount)      
								   ELSE 0
						END),2)																as aw_item_net_amt
					,round(sum(CASE
								  WHEN l.line_Action = 11 
									and  l.line_object_type = 5  
									AND line_void_flag = '0'
								  THEN gross_line_amount              
								  ELSE 0								        
						END),2)																as aw_item_tax_amt
					,round(sum(CASE        
								WHEN l.line_Action = 1 
									AND l.line_object_type = 1 
									AND line_void_flag = '0'
									and gross_line_amount < 0  
								THEN (gross_line_amount-pos_discount_amount) 
								WHEN l.line_Action = 2 
									AND l.line_object_type = 1 
									AND line_void_flag = '0' 
									and gross_line_amount >= 0 
								THEN (gross_line_amount-pos_discount_amount)
								ELSE 0
						END),2)																as aw_item_ret_amt
				from  `edl_stage.pre_aw_transaction_line` l				
				group by 1
			)ln_amt
			on ln_amt.if_entry_no  =  h.if_entry_no
      left join (select * from
								(select 
									fcurr_curr
									,ukurs_curr 
									,gdatu_inv
									,row_number() OVER (PARTITION BY fcurr_curr,gdatu_inv order by batch_id desc) rn_curr
								from edl_stage.pre_sap_exchrate exc          
								)exc 
						where rn_curr = 1)
				on  fcurr_curr = h.transaction_remark
				and gdatu_inv = CAST(FORMAT_TIMESTAMP("%Y%m%d", h.entry_date_time) AS STRING)  
	where rn=1 
) AS h
LEFT OUTER JOIN edl_stage.pre_ann_calendar cal
  ON (CAST(cal.day_dt AS DATE) = h.transaction_dt)