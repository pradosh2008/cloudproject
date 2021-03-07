create or replace table edl_conform.transaction
as SELECT
transaction_key
,store_id
,transaction_num
,register_num
,transaction_dt
,store_key
,case when CHAR_LENGTH(transaction_tm)=1 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(transaction_dt as string),' ','000',transaction_tm))
			when CHAR_LENGTH(transaction_tm)=2 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(transaction_dt as string),' ','00',transaction_tm))
			when CHAR_LENGTH(transaction_tm)=3 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(transaction_dt as string),' ','0',transaction_tm))
			when CHAR_LENGTH(transaction_tm)=4 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(transaction_dt as string),' ',transaction_tm))
       else PARSE_DATETIME('%Y-%m-%d %H%M%S', concat(cast(transaction_dt as string),' ',transaction_tm))
       end as transaction_tms
,channel_cd
,transaction_type_cd
,transaction_entry_type_cd
,cashier_id
,store_manager_id
,layaway_num
,multi_brand_transaction_ind
,domestic_order_ind
,address_capture_ind
,email_capture_ind
,return_receipt_ind
,order_num
,order_receive_dt
,case when CHAR_LENGTH(order_receive_tm)=1 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(order_receive_dt as string),' ','000',order_receive_tm))
			when CHAR_LENGTH(order_receive_tm)=2 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(order_receive_dt as string),' ','00',order_receive_tm))
			when CHAR_LENGTH(order_receive_tm)=3 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(order_receive_dt as string),' ','0',order_receive_tm))
			when CHAR_LENGTH(order_receive_tm)=4 then PARSE_DATETIME('%Y-%m-%d %H%M', concat(cast(order_receive_dt as string),' ',order_receive_tm))
       else PARSE_DATETIME('%Y-%m-%d %H%M%S', concat(cast(order_receive_dt as string),' ',order_receive_tm))
       end as order_tms
,transaction_void_ind
,ship_to_store_key
,membership_ind
,plcc_payment_reference_nbr
,plcc_payment_line_object_cd
,plcc_payment_tm
,plcc_payment_amt
,p2e_ind
,edl_create_tms
,edl_create_job_nam
,edl_last_update_tms
,edl_last_update_job_nam
,brand_cd
FROM edl_conform.transaction;

