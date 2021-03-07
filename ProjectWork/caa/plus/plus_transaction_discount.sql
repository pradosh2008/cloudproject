create or replace TABLE work.plus_transaction_discount as
SELECT
TO_HEX(MD5(concat(cast(case when dis.selling_chain_nbr = '7' then 'LB' else 'CA' end as string)
    ,'|',cast(dis.selling_store_nbr as string)
    ,'|',cast(dis.register_nbr as string)
    ,'|',cast(dis.transaction_dt as string)
    ,'|',cast(dis.transaction_nbr as string)))) as transaction_key
,dis.detail_fee_seq_nbr as transaction_line_num
,dis.discount_seq_nbr as discount_line_num
,TO_HEX(MD5(concat(cast(case when dis.selling_chain_nbr = '7' then 'LB' else 'CA' end as string),'|',cast(trn.customer_nbr as string)))) as brand_customer_key	
,TO_HEX(MD5(concat(cast(case when dis.selling_chain_nbr = '7' then 'LB' else 'CA' end as string),'|',cast(cast(dis.sku_nbr as INT64) as STRING)))) as item_key	
,TO_HEX(MD5(concat(cast(case when dis.selling_chain_nbr = '7' then 'LB' else 'CA' end as string),'|',cast(dis.selling_store_nbr as string)))) as store_key
,case when trn.order_received_dt is null or trim(trn.order_received_dt)='' then dis.transaction_dt else trn.order_received_dt end as order_dt
, case 
      when cast(det.sales_return_ind as INT64) = 1 and (det.original_store_nbr is not null and det.original_register_nbr is not null and det.original_transaction_dt is not null and det.original_transaction_nbr is not null ) then TO_HEX(MD5(concat(cast(case when det.selling_chain_nbr = 7 then 'LB' else 'CA' end as string)
                                                               ,'|',cast(det.original_store_nbr as string)
                                                               ,'|',cast(det.original_register_nbr as string)
                                                               ,'|',cast(det.original_transaction_dt as string)
                                                               ,'|',cast(det.original_transaction_nbr as string))))
      ELSE NULL 
      END as original_transaction_header_key   -- Needs more analysis
, case when cast(det.sales_return_ind as INT64) = 1 AND det.original_store_nbr is not null 
			then TO_HEX(MD5(concat(cast(case when det.selling_chain_nbr = 7 then 'LB' else 'CA' end as string),'|',cast(det.original_store_nbr as string)))) 
			ELSE NULL 
			END 
  as original_transaction_store_key
--, case whendet.return_ind = 1 then det.original_transaction_register_num ELSE NULL END as original_transaction_register_num
, case when cast(det.sales_return_ind as INT64) = 1 and det.original_transaction_dt is not null then det.original_transaction_dt ELSE NULL END as original_transaction_dt
--, case whendet.return_ind = 1 then det.orinigal_transaction_num ELSE NULL END as orinigal_transaction_num
--, case whendet.return_ind = 1 then det.original_transaction_line_num ELSE NULL END as orinigal_transaction_line_num
, null as orinigal_transaction_line_num
, case when cast(det.sales_return_ind as INT64) = 1 and det.ret_primary_sales_person_nbr is not null then det.ret_primary_sales_person_nbr ELSE NULL END as original_primary_salesperson_id
, case when cast(det.sales_return_ind as INT64) = 1 and det.ret_secondary_sales_person_nbr is not null then det.ret_secondary_sales_person_nbr ELSE NULL END as original_secondary_salesperson_id
, case when trn.transaction_dt IS NULL then trn.order_received_dt ELSE  trn.transaction_dt END as transaction_dt
, dis.coupon_cd
, dis.discount_amt as discount_amt
, dis.discount_reason_cd
, dis.discount_sub_reason_cd
, dis.pos_event_cd
, dis.discount_type_cd
, dis.deal_cd
, dis.discount_method_cd
, CURRENT_TIMESTAMP as edl_create_tms
, 'TRANS' as edl_create_job_nam
, CURRENT_TIMESTAMP as edl_last_update_tms
, 'TRANS' as edl_last_update_job_nam
, cast(case when dis.selling_chain_nbr = '7' then 'LB' else 'CA' end as string) as brand_cd
from edl_landing.lb_sales_transaction_discount dis 
left outer join edl_landing.lb_sales_transaction_detail det on
trim(dis.transaction_dt) = COALESCE(det.transaction_dt,'')
AND cast(trim(dis.transaction_nbr) as INT64) = det.transaction_nbr
AND cast(trim(dis.selling_chain_nbr) as INT64) = det.selling_chain_nbr
AND cast(trim(dis.register_nbr) as INT64) = det.register_nbr
AND cast(trim(dis.selling_store_nbr) as INT64) = det.selling_store_nbr
AND cast(trim(dis.detail_fee_seq_nbr) as INT64) = det.detail_seq_nbr
left outer join edl_landing.lb_sales_transaction_header trn ON
   dis.transaction_dt = COALESCE(trn.transaction_dt,'')
   AND cast(trim(dis.transaction_nbr) as INT64) = trn.transaction_nbr
   AND cast(trim(dis.selling_chain_nbr) as INT64) = trn.selling_chain_nbr
   AND cast(trim(dis.register_nbr) as INT64) = trn.register_nbr
   AND cast(trim(dis.selling_store_nbr) as INT64) = trn.selling_store_nbr;