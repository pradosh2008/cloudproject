create or replace table edl_conform.transaction
as SELECT transaction_key
,store_id
,transaction_num
,register_num
,transaction_dt
,store_key
,transaction_tm
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
,order_receive_tm
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
FROM (
  SELECT
transaction_key
,store_id
,transaction_num
,register_num
,transaction_dt
,store_key
,transaction_tm
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
,order_receive_tm
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
,
      ROW_NUMBER()
          OVER (PARTITION BY transaction_key)
          row_number
  FROM edl_conform.transaction
)
WHERE row_number = 1;