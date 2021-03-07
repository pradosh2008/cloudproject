. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh

#Take an archive of the mart table of previous load
bq cp --force analytic_mart.plus_transaction edl_archive.plus_transaction
rc_check $? "Archive copy"

#Load Data from staging table into analytic_mart

bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table analytic_mart.plus_transaction --use_legacy_sql=false <<!
create or replace table work.transaction as
SELECT
TO_HEX(MD5(concat(CAST(case when selling_chain_nbr = 7 then 'LB' else 'CA' end as string)
    ,'|',CAST (selling_store_nbr AS string)
    ,'|',CAST (register_nbr AS string)
    ,'|',CAST(transaction_dt as string)
    ,'|',CAST(transaction_nbr AS string)))) as transaction_key
,case when customer_nbr is NULL or customer_nbr=0 then '0' 
      else TO_HEX(MD5(CONCAT(CAST(case when selling_chain_nbr = 7 then 'LB' 
	                                   else 'CA' 
							      end as string),'|',cast(customer_nbr as string)))) 
end as brand_customer_key  --we need to bring the brand-customer_key from customer_transaction linkage table
,TO_HEX(MD5(concat(CAST(case when selling_chain_nbr = 7 then 'LB' else 'CA' end as string),'|',CAST(selling_store_nbr AS string)))) as store_key
,transaction_dt as transaction_dt
,case when order_received_dt is null then transaction_dt else order_received_dt end as order_dt
,transaction_nbr as transaction_num
,register_nbr as register_num
,sales_channel_type_cd as channel_cd   -- it looks different
,transaction_type_cd as transaction_type_cd
,entry_type_cd as transaction_entry_type_cd
,cashier_nbr as cashier_id
,manager_nbr as store_manager_id
,layaway_nbr as layaway_num
,case when trim(multi_brand_transaction_ind)='Y' then 1
      when trim(multi_brand_transaction_ind)='N' then 0
      else NULL end as multi_brand_transaction_ind
,case when trim(domestic_order_ind)='Y' then 1
      when trim(domestic_order_ind)='N' then 0
      else NULL end as domestic_order_ind	  
,case when trim(address_capture_ind)='Y' then 1
      when trim(address_capture_ind)='N' then 0
      else NULL end as address_capture_ind
,case when trim(email_capture_ind)='Y' then 1
      when trim(email_capture_ind)='N' then 0
      else NULL end as email_capture_ind
,case when trim(return_receipt_ind)='Y' then 1
      when trim(return_receipt_ind)='N' then 0
      else NULL end as return_receipt_ind
,order_nbr as order_num
,transaction_void_ind as transaction_void_ind
,case when ship_to_store_nbr is not null and ship_to_store_nbr!=0 
		   then TO_HEX(MD5(concat(CAST(case when selling_chain_nbr = 7 then 'LB' else 'CA' end as string),'|',cast(ship_to_store_nbr as string)))) 
      else NULL 
end as ship_to_store_key --This looks susceptible
,case when tender_reference_cd is not null and trim(tender_reference_cd) != '0' and trim(tender_reference_cd)!='' then 1 else 0 end as membership_ind
,payment_reference_nbr as plcc_payment_reference_nbr --Should we pull this from EDW table called SALES_TRANSACTION_PAYMENT
,null as plcc_payment_line_object_cd
,transaction_tm as plcc_payment_tm   -- Should we pull this from EDW table called SALES_TRANSACTION_PAYMENT
,payment_amt as plcc_payment_amt     -- Should we pull this from EDW table called SALES_TRANSACTION_PAYMENT
,null as p2e_ind
,CURRENT_TIMESTAMP AS edl_create_tms
,'TRANS' AS edl_create_job_nam
,CURRENT_TIMESTAMP AS edl_last_update_tms
,'TRANS' AS edl_last_update_job_nam
,CAST(case when selling_chain_nbr = 7 then 'LB' else 'CA' end as string) as brand_cd
from edl_stage.plus_sales_transaction_header
;

rc_check $? "Loading mart transaction table"