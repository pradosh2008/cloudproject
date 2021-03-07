select	
FARM_FINGERPRINT(concat(CAST(h.store_no AS string)
,'|',CAST (h.register_no AS string)
,'|',CAST(FORMAT_TIMESTAMP("%Y%m%d", h.entry_date_time) AS STRING)
,'|',cast(h.transaction_no AS string)
,'|',cast(l.line_id as string)
                  )) as transaction_item_key,
FARM_FINGERPRINT(concat(CAST(h.store_no AS string)
,'|',CAST (h.register_no AS string)
,'|',CAST(FORMAT_TIMESTAMP("%Y%m%d", h.entry_date_time) AS STRING)
,'|',cast(h.transaction_no AS string)
                  )) as transaction_key,
CONCAT(CAST(h.store_no AS string)
,':',CAST (h.register_no AS string)
,':',CAST(FORMAT_TIMESTAMP("%Y%m%d", h.entry_date_time) AS STRING)
,':',cast(h.transaction_no AS string)
                  ) as transaction_num
,l.line_id
,'AW' as transaction_source_cd
,FARM_FINGERPRINT(cast(store_no as string)) as store_key
,register_no as register_num
,EXTRACT(DATE FROM entry_date_time) as transaction_dt
,EXTRACT(TIME FROM entry_date_time) as transaction_tms
,'RET' as channel_cd
,m.sku_id as sku_id
,round(ticket_price,2) as list_price_amt
,
	CASE 
WHEN l.line_Action = 1 
and gross_line_amount >= 0 THEN 1
WHEN l.line_Action = 24 
and gross_line_amount >= 0 THEN 1
WHEN l.line_Action = 2 
and gross_line_amount < 0 THEN 1
ELSE 0 
	END as item_sold_qty
,
	CASE 
WHEN l.line_Action = 1 
and gross_line_amount >= 0 THEN ROUND(gross_line_amount,2)
WHEN l.line_Action = 24 
and gross_line_amount >= 0 THEN ROUND(gross_line_amount,2)
WHEN l.line_Action = 2 
and gross_line_amount < 0 THEN ROUND(gross_line_amount,2)
ELSE 0 
	END as item_sold_amt	
,
	CASE 
WHEN l.line_Action = 1 
and gross_line_amount >= 0 THEN ROUND(gross_line_amount,2)
WHEN l.line_Action = 2 
and gross_line_amount < 0 THEN ROUND(gross_line_amount,2)
ELSE 0 
	END as item_gross_amt
,
	CASE 
WHEN l.line_Action = 1 
and gross_line_amount >= 0 THEN ROUND(pos_discount_amount,2)
WHEN l.line_Action = 2 
and gross_line_amount < 0 THEN ROUND(pos_discount_amount,2)
ELSE 0 
	END as item_disc_amt	
,
	CASE 
WHEN l.line_Action = 1 
and gross_line_amount >= 0 THEN ROUND(m.sold_at_price,2)
WHEN l.line_Action = 2 
and gross_line_amount < 0 THEN ROUND(m.sold_at_price,2)
ELSE 0 
	END as item_net_amt	
,r.return_from_transaction_no as item_return_from_transaction_num
,r.return_reason_code as item_return_cd
,r.return_from_date as item_return_dt
,
	CASE  
WHEN l.line_Action = 1  
and gross_line_amount < 0   THEN 1 
WHEN l.line_Action = 2  
and gross_line_amount >= 0  THEN 1
ELSE 0 
	END as item_return_qty
,
	CASE         
WHEN l.line_Action = 1  
and gross_line_amount < 0  THEN round(gross_line_amount-pos_discount_amount,2)  
WHEN l.line_Action = 2  
and gross_line_amount >= 0 THEN round(gross_line_amount-pos_discount_amount,2)
ELSE 0 
	END as item_return_amt
--,i.full_price_flg as full_price_flg
,l.batch_id
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
		 --begin for checking purpose     
		and CONCAT(CAST(store_no AS string)
			   ,':',CAST (register_no AS string)
			   ,':',CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING)
			   ,':',cast(transaction_no AS string))='1177:1:20181017:3922'
		--end for checking purpose
		--and EXTRACT(DATE FROM entry_date_time)>'2019-05-04'
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