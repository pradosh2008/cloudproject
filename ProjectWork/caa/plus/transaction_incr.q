------1.TRANSACTION_FEE
set tez.queue.name=ascena_batch_process;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.execution.engine=tez;
set hive.vectorized.execution.enabled = true;

INSERT INTO TABLE @CONFORM_DATABASE.TRANSACTION_FEE PARTITION (brand_cd)
SELECT
distinct 
md5(concat('LB|',  --Nishanth
lbstf.store_nbr,"|",
lbstf.register_nbr,"|",
lbstf.transaction_dt,"|",
lbstf.transaction_nbr))AS transaction_key
,lbstf.transaction_line_nbr AS transaction_fee_line_num
,lbstf.fee_nbr AS fee_cd
,lbstf.fee_amt AS fee_amt
,0 AS discount_amt  --Nishanth
,lbstf.non_merch_tax_type AS non_merchandise_tax_type_cd
,lbstf.non_merch_prorated_tax_amt AS non_merchandise_prorated_tax_amt
,lbstf.non_merch_tax_amt AS non_merchandise_tax_amt
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'LB' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_lb_sales_transaction_fee lbstf 
WHERE lbstf.chain_nbr=7  --Nishanth
AND lbstf.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd')
UNION 
SELECT 
md5(concat('CA|',  --Nishanth
castf.store_nbr,"|",
castf.register_nbr,"|",
castf.transaction_dt,"|",
castf.transaction_nbr))AS transaction_key
,castf.transaction_line_nbr AS transaction_fee_line_num
,castf.fee_nbr AS fee_cd
,castf.fee_amt AS fee_amt
,0 AS discount_amt  --Nishanth
,castf.non_merch_tax_type AS non_merchandise_tax_type_cd
,castf.non_merch_prorated_tax_amt AS non_merchandise_prorated_tax_amt
,castf.non_merch_tax_amt AS non_merchandise_tax_amt
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'CA' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_ca_sales_transaction_fee castf 
WHERE castf.chain_nbr IN (4)  --Nishanth
AND castf.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd');


---------2.TRANSACTION_TAX

INSERT INTO TABLE @CONFORM_DATABASE.TRANSACTION_TAX PARTITION (brand_cd)
SELECT 
md5(concat('LB|',  --Nishanth
lbstt.store_nbr,"|",
lbstt.register_nbr,"|",
lbstt.transaction_dt,"|",
lbstt.transaction_nbr))AS transaction_key
,lbstt.tax_line_nbr AS tax_line_num
,lbstt.tax_category_nbr AS tax_category_num
,lbstt.tax_jurisdiction_cd 
,lbstt.tax_level_nbr AS tax_level_num
,lbstt.line_action_cd 
,lbstt.line_object_type_cd 
,lbstt.line_object_cd
,lbstt.tax_rt
,case when line_action_cd = '11' then lbstt.customer_paid_tax_amt when line_action_cd = '12' then (-1 * lbstt.customer_paid_tax_amt) else lbstt.customer_paid_tax_amt end as customer_paid_tax_amt
,case when line_action_cd = '11' then lbstt.company_owed_tax_amt when line_action_cd = '12' then (-1 * lbstt.company_owed_tax_amt) else lbstt.company_owed_tax_amt end as company_owed_tax_amt
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'LB' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_lb_sales_transaction_tax lbstt
WHERE lbstt.chain_nbr IN (7)
and lbstt.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd')
UNION
SELECT 
md5(concat('CA|',  --Nishanth
castt.store_nbr,"|",
castt.register_nbr,"|",
castt.transaction_dt,"|",
castt.transaction_nbr))AS transaction_key
,castt.tax_line_nbr AS tax_line_num
,castt.tax_category_nbr AS tax_category_num
,castt.tax_jurisdiction_cd 
,castt.tax_level_nbr AS tax_level_num
,castt.line_action_cd 
,castt.line_object_type_cd 
,castt.line_object_cd
,castt.tax_rt
,case when line_action_cd = '11' then castt.customer_paid_tax_amt when line_action_cd = '12' then (-1 * castt.customer_paid_tax_amt) else castt.customer_paid_tax_amt end as customer_paid_tax_amt
,case when line_action_cd = '11' then castt.company_owed_tax_amt when line_action_cd = '12' then (-1 * castt.company_owed_tax_amt) else castt.company_owed_tax_amt end as company_owed_tax_amt
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'CA' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_ca_sales_transaction_tax castt
WHERE castt.chain_nbr IN (4)  --Nishanth
AND castt.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd');


--------------3.TRANSACTION_NOTES

INSERT INTO TABLE @CONFORM_DATABASE.TRANSACTION_NOTES PARTITION (brand_cd)
SELECT 
md5(concat('LB|',  --Nishanth
lbstn.store_nbr,"|",
lbstn.register_nbr,"|",
lbstn.transaction_dt,"|",
lbstn.transaction_nbr))AS transaction_key
,lbstn.notes_line_nbr AS transaction_notes_line_num
,lbstn.notes_seq_nbr AS transaction_notes_sequence_num
,lbstn.note_type_desc AS note_type_desc
,lbstn.free_form_note_desc AS free_form_notes_desc
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'LB' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_lb_sales_transaction_notes lbstn
WHERE lbstn.chain_nbr IN (7)  --Nishanth
AND lbstn.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd')
UNION
SELECT 
md5(concat('CA|',  --Nishanth
castn.store_nbr,"|",
castn.register_nbr,"|",
castn.transaction_dt,"|",
castn.transaction_nbr))AS transaction_key
,castn.notes_line_nbr AS transaction_notes_line_num
,castn.notes_seq_nbr AS transaction_notes_sequence_num
,castn.note_type_desc AS note_type_desc
,castn.free_form_note_desc AS free_form_notes_desc
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'CA' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_ca_sales_transaction_notes castn  --Nishanth
WHERE castn.chain_nbr IN (4)  --Nishanth
AND castn.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd');


------------4.TRANSACTION_TENDER

INSERT INTO TABLE @CONFORM_DATABASE.TRANSACTION_TENDER PARTITION (brand_cd)
SELECT 
md5(concat('LB|',  --Nishanth
lbsttd.store_nbr,"|",
lbsttd.register_nbr,"|",
lbsttd.transaction_dt,"|",
lbsttd.transaction_nbr))AS transaction_key
,lbsttd.tender_sequence_nbr AS tender_line_num
,case when line_action_cd in (11,25,28) then lbsttd.tender_amt else (lbsttd.tender_amt * -1) end as tender_amt  -- Changed by Nishanth -- 12-APR-2019 -- to handle -ve value for tender_amt based on line_action_cd
,lbsttd.tender_expiration_dt
,CASE WHEN lbsttd.tender_cd = '' THEN NULL ELSE lbsttd.tender_cd END AS tender_cd
,CASE WHEN lbsttd.tender_reference_nbr = '' THEN NULL ELSE lbsttd.tender_reference_nbr END  AS tender_reference_num
,lbsttd.tender_void_ind
,lbsttd.deferred_billing_dt
,CASE WHEN lbsttd.paypal_order_nbr ='' THEN NULL ELSE lbsttd.paypal_order_nbr END AS paypal_order_id
,lbsttd.tender_salesperson_nbr AS tender_salesperson_key
,CASE WHEN lbsttd.authorization_nbr ='' THEN NULL ELSE lbsttd.authorization_nbr END AS authorization_id
,case when trim(lbsttd.authorize_key_flg)='' THEN NULL
when trim(lbsttd.authorize_key_flg)='N' THEN 0
when trim(lbsttd.authorize_key_flg)='Y' THEN 1 END AS authorize_key_ind
,CASE WHEN lbsttd.deferred_billing_plan_nbr ='' THEN NULL ELSE lbsttd.deferred_billing_plan_nbr END AS deferred_billing_plan_id
,lbsttd.open_charge_ind
,CASE WHEN lbsttd.charge_application_status_cd ='' THEN NULL ELSE lbsttd.charge_application_status_cd END AS charge_application_status_cd
,CASE WHEN lbsttd.state_cd ='' THEN NULL ELSE lbsttd.state_cd END AS state_cd            
,lbsttd.create_dt AS record_create_tm    
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'LB' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_lb_sales_transaction_tender lbsttd
WHERE lbsttd.chain_nbr IN (7)  --Nishanth
AND lbsttd.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd')
UNION
SELECT 
md5(concat('CA|',  --Nishanth
casttd.store_nbr,"|",
casttd.register_nbr,"|",
casttd.transaction_dt,"|",
casttd.transaction_nbr))AS transaction_key
,casttd.tender_sequence_nbr AS tender_line_num
,case when line_action_cd in (11,25,28) then casttd.tender_amt else (casttd.tender_amt * -1) end as tender_amt  -- Changed by Nishanth -- 12-APR-2019 -- to handle -ve value for tender_amt based on line_action_cd
,casttd.tender_expiration_dt
,CASE WHEN casttd.tender_cd = '' THEN NULL ELSE casttd.tender_cd END AS tender_cd
,CASE WHEN casttd.tender_reference_nbr = '' THEN NULL ELSE casttd.tender_reference_nbr END  AS tender_reference_num
,casttd.tender_void_ind
,casttd.deferred_billing_dt
,CASE WHEN casttd.paypal_order_nbr ='' THEN NULL ELSE casttd.paypal_order_nbr END AS paypal_order_id
,casttd.tender_salesperson_nbr AS tender_salesperson_key
,CASE WHEN casttd.authorization_nbr ='' THEN NULL ELSE casttd.authorization_nbr END AS authorization_id
,case when trim(casttd.authorize_key_flg)='' THEN NULL
when trim(casttd.authorize_key_flg)='N' THEN 0
when trim(casttd.authorize_key_flg)='Y' THEN 1 END AS authorize_key_ind
,CASE WHEN casttd.deferred_billing_plan_nbr ='' THEN NULL ELSE casttd.deferred_billing_plan_nbr END AS deferred_billing_plan_id
,casttd.open_charge_ind
,CASE WHEN casttd.charge_application_status_cd ='' THEN NULL ELSE casttd.charge_application_status_cd END AS charge_application_status_cd
,CASE WHEN casttd.state_cd ='' THEN NULL ELSE casttd.state_cd END AS state_cd            
,casttd.create_dt AS record_create_tm    
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'CA' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_ca_sales_transaction_tender casttd  --Nishanth
WHERE casttd.chain_nbr IN (4)
AND casttd.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd');


-------------5.TRANSACTION_DISCOUNT

INSERT INTO TABLE @CONFORM_DATABASE.TRANSACTION_DISCOUNT PARTITION (brand_cd)
SELECT 
md5(concat('LB|',  --Nishanth
lbstd.store_nbr,"|",
lbstd.register_nbr,"|",
lbstd.transaction_dt,"|",
lbstd.transaction_nbr))AS transaction_key
,lbstd.transaction_line_nbr AS transaction_line_num
,lbstd.discount_sequence_nbr AS discount_line_num
,substr(lbstn.free_form_note_desc,23,20) AS coupon_cd  --Nishanth
,lbstd.discount_amt
,case when trim(lbstd.discount_reason_cd) ='' then NULL else lbstd.discount_reason_cd END AS discount_reason_cd
,case when discount_sub_reason_cd ='' then NULL else lbstd.discount_sub_reason_cd END AS discount_sub_reason_cd
,case when lbstd.pos_event_cd='' then NULL else lbstd.pos_event_cd END AS point_of_sale_event_cd
,case when trim(lbstd.discount_type_cd)='' then NULL when lbstd.discount_type_cd="?" then NULL else lbstd.discount_type_cd END AS discount_type_cd
,case when lbstd.deal_cd= '' then NULL else lbstd.deal_cd END AS deal_cd
,case when trim(lbstd.discount_method_cd)= '' then NULL else lbstd.discount_method_cd END AS discount_method_cd
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'LB' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_lb_sales_transaction_discount lbstd
left outer join  --Nishanth
(select * from @STAGING_DATABASE.lbca_lb_sales_transaction_notes where note_type_cd=43) lbstn  --Nishanth
on lbstd.store_nbr=lbstn.store_nbr  --Nishanth
and lbstd.register_nbr=lbstn.register_nbr  --Nishanth
and lbstd.transaction_dt=lbstn.transaction_dt  --Nishanth
and lbstd.transaction_nbr=lbstn.transaction_nbr  --Nishanth
and lbstd.applied_by_line_nbr=lbstn.notes_line_nbr  --Nishanth
where lbstd.chain_nbr IN (7)
and lbstd.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd')
UNION
SELECT 
md5(concat('CA|',  --Nishanth
castd.store_nbr,"|",
castd.register_nbr,"|",
castd.transaction_dt,"|",
castd.transaction_nbr))AS transaction_key
,castd.transaction_line_nbr AS transaction_line_num
,castd.discount_sequence_nbr AS discount_line_num
,substr(castn.free_form_note_desc,23,20) AS coupon_cd  --Nishanth
,castd.discount_amt
,case when trim(castd.discount_reason_cd) ='' then NULL else castd.discount_reason_cd END AS discount_reason_cd
,case when discount_sub_reason_cd ='' then NULL else castd.discount_sub_reason_cd END AS discount_sub_reason_cd
,case when castd.pos_event_cd='' then NULL else castd.pos_event_cd END AS point_of_sale_event_cd
,case when trim(castd.discount_type_cd)='' then NULL when castd.discount_type_cd="?" then NULL else castd.discount_type_cd END AS discount_type_cd
,case when castd.deal_cd= '' then NULL else castd.deal_cd END AS deal_cd
,case when trim(castd.discount_method_cd)= '' then NULL else castd.discount_method_cd END AS discount_method_cd
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'CA' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_ca_sales_transaction_discount castd
left outer join  --Nishanth
(select * from @STAGING_DATABASE.lbca_ca_sales_transaction_notes where note_type_cd=43) castn  --Nishanth
on castd.store_nbr=castn.store_nbr  --Nishanth
and castd.register_nbr=castn.register_nbr  --Nishanth
and castd.transaction_dt=castn.transaction_dt  --Nishanth
and castd.transaction_nbr=castn.transaction_nbr  --Nishanth
and castd.applied_by_line_nbr=castn.notes_line_nbr  --Nishanth
where castd.chain_nbr IN (4)  --Nishanth
and castd.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd');


----------------6.TRANSACTION_DETAIL

INSERT INTO TABLE @CONFORM_DATABASE.TRANSACTION_DETAIL PARTITION (brand_cd)
SELECT 
distinct  
md5(concat('LB|',  --Nishanth
lbstdl.store_nbr,"|",
lbstdl.register_nbr,"|",
lbstdl.transaction_dt,"|",
lbstdl.transaction_nbr))AS transaction_key
,CASE when lbstdl.transaction_line_nbr = '' then NULL else lbstdl.transaction_line_nbr END AS transaction_line_num
,CASE when lbstdl.line_action_cd = '' then NULL else lbstdl.line_action_cd END AS line_action_cd
,CASE when lbstdl.line_object_type_cd = '' then NULL else lbstdl.line_object_type_cd END AS line_object_type_cd
,CASE when lbstdl.line_object_cd = '' then NULL else lbstdl.line_object_cd END AS line_object_cd
,lbstdl.line_void_ind
,case when sku_nbr=0 then 0 else md5(concat ('LB|',LPAD(cast(lbstdl.SKU_NBR AS STRING),9,0))) end as item_key  --Nishanth
,lbstdl.sku_cost_amt AS sku_cost_amt
,lbstdl.sku_quantity_num AS sku_quantity_num
,lbstdl.sku_retail_amt AS sku_retail_amt
,CASE when trim(lbstdl.permanent_markdown_type_cd)  = '' then NULL else trim(lbstdl.permanent_markdown_type_cd) END AS permanent_markdown_type_cd
,CASE when lbstdl.pos_markdown_rtl = '' then NULL else lbstdl.pos_markdown_rtl END AS markdown_amt 
,lbstdl.customer_paid_tax_amt AS customer_paid_tax_amt
,CASE when lbstdl.company_tax_amt = '' then NULL else lbstdl.company_tax_amt END AS company_owed_tax_amt
,CASE when trim(lbstdl.return_reason_cd) = '' then NULL else trim(lbstdl.return_reason_cd) END AS return_reason_cd
,lbstdl.commision_amt AS commision_amt
,CASE when lbstdl.current_rtl ='' then NULL else lbstdl.current_rtl END AS current_retail_amt
,CASE when lbstdl.original_unit_rtl = '' then NULL else lbstdl.original_unit_rtl END AS original_unit_retail_amt
,CASE when trim(lbstdl.scanned_ind) = '' then NULL else trim(lbstdl.scanned_ind) END AS scanned_ind
,case when sku_nbr=0 then 1 else 0 end AS gift_card_ind
,case when trim(lbstdl.taxable_ind)='Y' THEN 1
 when trim(lbstdl.taxable_ind)='N' THEN 0
 when trim(lbstdl.taxable_ind)='' then NULL end  AS taxable_ind
,case when trim(lbstdl.style_locator_ind)='Y' THEN 1
 when trim(lbstdl.style_locator_ind)='N' THEN 0
 when trim(lbstdl.style_locator_ind)='' then NULL end  AS style_locator_ind
,case when lbstdl.primary_salesperson_nbr ='' then NULL else lbstdl.primary_salesperson_nbr end  AS primary_salesperson_id
,case when lbstdl.secondary_salesperson_nbr ='' then NULL else lbstdl.secondary_salesperson_nbr end AS secondary_salesperson_id
,CASE when lbstdl.SALES_RETURN_IND = 'Y' then '1'
 when lbstdl.SALES_RETURN_IND='N' THEN 0
 when lbstdl.SALES_RETURN_IND='' then NULL end AS return_ind 
,case when lbstdl.SALES_RETURN_IND='Y' then  --Nishanth
md5(concat ('LB|',
lbstr.original_store_nbr,"|",
coalesce(lbstr.original_register_nbr,""),"|",
coalesce(cast(lbstr.original_transaction_dt as char(10)),""),"|",
coalesce(lbstr.original_transaction_nbr,""))) 
else NULL end as original_transaction_header_key  --Nishanth
,CASE when lbstdl.SALES_RETURN_IND='Y' then lbstr.original_store_nbr else NULL end as original_transaction_store_id  --Nishanth
,CASE when lbstdl.SALES_RETURN_IND='Y' then lbstr.original_register_nbr else null end as original_transaction_register_num  --Nishanth
,CASE when lbstdl.SALES_RETURN_IND='Y' then lbstr.original_transaction_dt else null end as original_transaction_dt  --Nishanth
,CASE when lbstdl.SALES_RETURN_IND='Y' then lbstr.original_transaction_nbr else null end as orinigal_transaction_num  --Nishanth
,lbstdl.transaction_line_nbr AS original_transaction_line_num
,case when lbstr.primary_sales_person_nbr = '' then NULL else lbstr.primary_sales_person_nbr end AS original_primary_salesperson_id
,case when lbstr.secondary_sales_person_nbr ='' then NULL else lbstr.secondary_sales_person_nbr end AS original_secondary_salesperson_id
,case when lbstr.return_disposition_cd = '' then NULL else lbstr.return_disposition_cd end AS return_disposition_cd
,case when lbstr.return_reason_desc = '' then NULL else lbstr.return_reason_desc end AS return_reason_desc
,lbstr.return_to_dc_ind AS return_to_dc_ind
,lbstdl.sales_layway_ind AS layaway_ind
,lbstdl.sales_order_ind AS order_ind
,NULL AS upc_store_key
,lbstdl.upc_nbr AS upc_num
,NULL AS size_cd 
,md5(concat(coalesce(lbstdl.owning_store_nbr,""),"|",coalesce(lbstdl.owning_chain_nbr,""))) AS cross_brand_store_key
,lbstdl.OVERSEAS_ITEM_IND AS overseas_item_ind
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam
,CURRENT_TIMESTAMP AS edl_last_update_tms
,'TRANS' AS edl_last_update_job_nam
,'LB' AS brand_cd 
FROM 
(select 
chain_nbr
,store_nbr
,register_nbr
,transaction_dt
,transaction_nbr
,transaction_line_nbr
,line_action_cd
,line_object_type_cd
,line_object_cd
,line_void_ind
,SKU_NBR
,permanent_markdown_type_cd
,case when SALES_RETURN_IND='Y' then pos_markdown_rtl else (pos_markdown_rtl * -1) end as pos_markdown_rtl
,company_tax_amt
,return_reason_cd
,current_rtl
,original_unit_rtl
,scanned_ind
,taxable_ind
,style_locator_ind
,primary_salesperson_nbr
,secondary_salesperson_nbr
,SALES_RETURN_IND
,sales_layway_ind
,sales_order_ind
,upc_nbr
,owning_store_nbr
,owning_chain_nbr
,OVERSEAS_ITEM_IND
,TRANSACTION_LINE_QTY AS sku_quantity_num  --04/09/19 Nishanth
,TRANSACTION_LINE_CST  AS sku_cost_amt  --04/09/19 Nishanth  --04/12/19
,case when SALES_RETURN_IND='Y' then (-1 * TRANSACTION_LINE_RTL) else  TRANSACTION_LINE_RTL end  AS sku_retail_amt  --04/09/19 Nishanth  --04/12/19
,COMMISSION_AMT AS commision_amt  --04/09/19 Nishanth
,CUSTOMER_TAX_AMT AS customer_paid_tax_amt  --04/09/19 Nishanth
,batch_id
FROM @STAGING_DATABASE.lbca_lb_sales_transaction_detail lbstd1
--group by 
--chain_nbr,store_nbr,register_nbr,transaction_dt,transaction_nbr,transaction_line_nbr,line_action_cd,line_object_type_cd,line_object_cd,line_void_ind,sku_nbr,permanent_markdown_type_cd,pos_markdown_rtl,company_tax_amt,return_reason_cd,current_rtl,original_unit_rtl,scanned_ind,taxable_ind,style_locator_ind,primary_salesperson_nbr,secondary_salesperson_nbr,SALES_RETURN_IND,sales_layway_ind,sales_order_ind,upc_nbr,owning_store_nbr,owning_chain_nbr,overseas_item_ind,batch_id
)lbstdl
LEFT JOIN 
@STAGING_DATABASE.LBCA_LB_SALES_TRANSACTION_RETURN lbstr
ON md5(concat (lbstdl.chain_nbr,"|",lbstdl.store_nbr,"|",lbstdl.register_nbr,"|",lbstdl.transaction_dt,"|",lbstdl.transaction_nbr,"|",lbstdl.transaction_line_nbr)) = md5(concat (lbstr.chain_nbr,"|",lbstr.store_nbr,"|",lbstr.register_nbr,"|",lbstr.transaction_dt,"|",lbstr.transaction_nbr,"|",lbstr.return_line_nbr))  --Nishanth
where lbstdl.chain_nbr IN (7)
AND lbstdl.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd')
UNION
SELECT 
distinct
md5(concat('CA|',  --Nishanth
castdl.store_nbr,"|",
castdl.register_nbr,"|",
castdl.transaction_dt,"|",
castdl.transaction_nbr))AS transaction_key
,CASE when castdl.transaction_line_nbr = '' then NULL else castdl.transaction_line_nbr END AS transaction_line_num
,CASE when castdl.line_action_cd = '' then NULL else castdl.line_action_cd END AS line_action_cd
,CASE when castdl.line_object_type_cd = '' then NULL else castdl.line_object_type_cd END AS line_object_type_cd
,CASE when castdl.line_object_cd = '' then NULL else castdl.line_object_cd END AS line_object_cd
,castdl.line_void_ind
,case when sku_nbr=0 then 0 else md5(concat ('CA|',LPAD(cast(castdl.SKU_NBR AS STRING),9,0))) end as item_key  --Nishanth
,castdl.sku_cost_amt AS sku_cost_amt
,castdl.sku_quantity_num AS sku_quantity_num
,castdl.sku_retail_amt AS sku_retail_amt
,CASE when trim(castdl.permanent_markdown_type_cd)  = '' then NULL else trim(castdl.permanent_markdown_type_cd) END AS permanent_markdown_type_cd
,CASE when castdl.pos_markdown_rtl = '' then NULL else castdl.pos_markdown_rtl END AS markdown_amt 
,castdl.customer_paid_tax_amt AS customer_paid_tax_amt
,CASE when castdl.company_tax_amt = '' then NULL else castdl.company_tax_amt END AS company_owed_tax_amt
,CASE when trim(castdl.return_reason_cd) = '' then NULL else trim(castdl.return_reason_cd) END AS return_reason_cd
,castdl.commision_amt AS commision_amt
,CASE when castdl.current_rtl ='' then NULL else castdl.current_rtl END AS current_retail_amt
,CASE when castdl.original_unit_rtl = '' then NULL else castdl.original_unit_rtl END AS original_unit_retail_amt
,CASE when trim(castdl.scanned_ind) = '' then NULL else trim(castdl.scanned_ind) END AS scanned_ind
,case when sku_nbr=0 then 1 else 0 end AS gift_card_ind
,case when trim(castdl.taxable_ind)='Y' THEN 1
 when trim(castdl.taxable_ind)='N' THEN 0
 when trim(castdl.taxable_ind)='' then NULL end  AS taxable_ind
,case when trim(castdl.style_locator_ind)='Y' THEN 1
 when trim(castdl.style_locator_ind)='N' THEN 0
 when trim(castdl.style_locator_ind)='' then NULL end  AS style_locator_ind
,case when castdl.primary_salesperson_nbr ='' then NULL else castdl.primary_salesperson_nbr end  AS primary_salesperson_id
,case when castdl.secondary_salesperson_nbr ='' then NULL else castdl.secondary_salesperson_nbr end AS secondary_salesperson_id
,CASE when castdl.SALES_RETURN_IND = 'Y' then '1'
 when castdl.SALES_RETURN_IND='N' THEN 0
 when castdl.SALES_RETURN_IND='' then NULL end AS return_ind 
,case when castdl.SALES_RETURN_IND='Y' then  --Nishanth
md5(concat ('CA|',
castr.original_store_nbr,"|",
coalesce(castr.original_register_nbr,""),"|",
coalesce(cast(castr.original_transaction_dt as char(10)),""),"|",
coalesce(castr.original_transaction_nbr,""))) 
else NULL end as original_transaction_header_key  --Nishanth
,CASE when castdl.SALES_RETURN_IND='Y' then castr.original_store_nbr else NULL end as original_transaction_store_id  --Nishanth
,CASE when castdl.SALES_RETURN_IND='Y' then castr.original_register_nbr else null end as original_transaction_register_num  --Nishanth
,CASE when castdl.SALES_RETURN_IND='Y' then castr.original_transaction_dt else null end as original_transaction_dt  --Nishanth
,CASE when castdl.SALES_RETURN_IND='Y' then castr.original_transaction_nbr else null end as orinigal_transaction_num  --Nishanth
,castdl.transaction_line_nbr AS original_transaction_line_num
,case when castr.primary_sales_person_nbr = '' then NULL else castr.primary_sales_person_nbr end AS original_primary_salesperson_id
,case when castr.secondary_sales_person_nbr ='' then NULL else castr.secondary_sales_person_nbr end AS original_secondary_salesperson_id
,case when castr.return_disposition_cd = '' then NULL else castr.return_disposition_cd end AS return_disposition_cd
,case when castr.return_reason_desc = '' then NULL else castr.return_reason_desc end AS return_reason_desc
,castr.return_to_dc_ind AS return_to_dc_ind
,castdl.sales_layway_ind AS layaway_ind
,castdl.sales_order_ind AS order_ind
,NULL AS upc_store_key
,castdl.upc_nbr AS upc_num
,NULL AS size_cd 
,md5(concat(coalesce(castdl.owning_store_nbr,""),"|",coalesce(castdl.owning_chain_nbr,""))) AS cross_brand_store_key
,castdl.OVERSEAS_ITEM_IND AS overseas_item_ind
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam
,CURRENT_TIMESTAMP AS edl_last_update_tms
,'TRANS' AS edl_last_update_job_nam
,'CA' AS brand_cd 
FROM 
(select 
chain_nbr
,store_nbr
,register_nbr
,transaction_dt
,transaction_nbr
,transaction_line_nbr
,line_action_cd
,line_object_type_cd
,line_object_cd
,line_void_ind
,SKU_NBR
,permanent_markdown_type_cd
,case when SALES_RETURN_IND='Y' then pos_markdown_rtl else (pos_markdown_rtl * -1) end as pos_markdown_rtl
,company_tax_amt
,return_reason_cd
,current_rtl
,original_unit_rtl
,scanned_ind
,taxable_ind
,style_locator_ind
,primary_salesperson_nbr
,secondary_salesperson_nbr
,SALES_RETURN_IND
,sales_layway_ind
,sales_order_ind
,upc_nbr
,owning_store_nbr
,owning_chain_nbr
,OVERSEAS_ITEM_IND
,TRANSACTION_LINE_QTY AS sku_quantity_num  --04/09/19 Nishanth
,TRANSACTION_LINE_CST  AS sku_cost_amt  --04/09/19 Nishanth  --04/12/19
,case when SALES_RETURN_IND='Y' then (-1 * TRANSACTION_LINE_RTL) else  TRANSACTION_LINE_RTL end  AS sku_retail_amt  --04/09/19 Nishanth  --04/12/19 
,COMMISSION_AMT AS commision_amt  --04/09/19 Nishanth
,CUSTOMER_TAX_AMT AS customer_paid_tax_amt  --04/09/19 Nishanth
,batch_id
FROM @STAGING_DATABASE.lbca_ca_sales_transaction_detail castdl
--group by 
--chain_nbr,store_nbr,register_nbr,transaction_dt,transaction_nbr,transaction_line_nbr,line_action_cd,line_object_type_cd,line_object_cd,line_void_ind,sku_nbr,permanent_markdown_type_cd,pos_markdown_rtl,company_tax_amt,return_reason_cd,current_rtl,original_unit_rtl,scanned_ind,taxable_ind,style_locator_ind,primary_salesperson_nbr,secondary_salesperson_nbr,SALES_RETURN_IND,sales_layway_ind,sales_order_ind,upc_nbr,owning_store_nbr,owning_chain_nbr,overseas_item_ind,batch_id
)castdl
LEFT JOIN 
@STAGING_DATABASE.LBCA_CA_SALES_TRANSACTION_RETURN castr
ON md5(concat (castdl.chain_nbr,"|",castdl.store_nbr,"|",castdl.register_nbr,"|",castdl.transaction_dt,"|",castdl.transaction_nbr,"|",castdl.transaction_line_nbr)) = md5(concat (castr.chain_nbr,"|",castr.store_nbr,"|",castr.register_nbr,"|",castr.transaction_dt,"|",castr.transaction_nbr,"|",castr.return_line_nbr))  --Nishanth
where castdl.chain_nbr IN (4)
AND castdl.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd');


----------------7.TRANSACTION

INSERT INTO TABLE @CONFORM_DATABASE.TRANSACTION PARTITION (brand_cd)
SELECT 
md5(concat('LB|',  --Nishanth
lbsth.store_nbr,"|",
lbsth.register_nbr,"|",
lbsth.transaction_dt,"|",
lbsth.transaction_nbr))AS transaction_key
,lbsth.store_nbr AS store_id
,lbsth.transaction_nbr AS transaction_num
,lbsth.register_nbr AS register_num
,lbsth.transaction_dt
,md5(concat ('LB|',lbsth.store_nbr)) as store_key  --Nishanth
,case when lbsth.transaction_tm = '' then NULL else lbsth.transaction_tm end AS transaction_tm
,case when lbsth.channel_cd = '' then NULL else lbsth.channel_cd end AS channel_cd
,case when lbsth.transaction_type_cd = '' then NULL else lbsth.transaction_type_cd end AS transaction_type_cd
,case when lbsth.entry_type_cd = '' then NULL else lbsth.entry_type_cd end AS transaction_entry_type_cd
,case when lbsth.cashier_nbr = '' then NULL else lbsth.cashier_nbr end AS cashier_id
,case when lbsth.manager_nbr = '' then NULL else lbsth.manager_nbr end AS store_manager_id
,case when lbsth.layaway_nbr = '' then NULL else lbsth.layaway_nbr end AS layaway_num
,case when trim(lbsth.multi_brand_transaction_ind)='Y' THEN 1
 when trim(lbsth.multi_brand_transaction_ind)='N' THEN 0
 when trim(lbsth.multi_brand_transaction_ind)='' then NULL end AS multi_brand_transaction_ind
,case when trim(lbsth.domestic_order_ind)='Y' THEN 1
 when trim(lbsth.domestic_order_ind)='N' THEN 0
 when trim(lbsth.domestic_order_ind)='' then NULL end AS domestic_order_ind
,case when trim(lbsth.address_capture_ind)='Y' THEN 1
 when trim(lbsth.address_capture_ind)='N' THEN 0
 when trim(lbsth.address_capture_ind)='' then NULL end AS address_capture_ind
,case when trim(lbsth.email_capture_ind)='Y' THEN 1
 when trim(lbsth.email_capture_ind)='N' THEN 0
 when trim(lbsth.email_capture_ind)='' then NULL end AS email_capture_ind 
,case when trim(lbsth.return_receipt_ind)='Y' THEN 1
 when trim(lbsth.return_receipt_ind)='N' THEN 0
 when trim(lbsth.return_receipt_ind)='' then NULL end AS return_receipt_ind 
,case when lbsth.order_nbr = '' then NULL else lbsth.order_nbr end AS order_num
,lbsth.order_received_dt AS order_receive_dt
,case when lbsth.order_received_tm = '' then NULL else lbsth.order_received_tm end AS order_receive_tm
,lbsth.transaction_void_ind
,case when lbsth.ship_to_store_nbr is not null and lbsth.ship_to_store_nbr<>0 then 
MD5(concat('CA|',COALESCE(lbsth.ship_to_store_nbr,''))) else NULL end AS ship_to_store_key --Nishanth
,case when lbsth.tender_reference_cd is not null and lbsth.tender_reference_cd <> 0 then "1" else "0" end AS membership_ind
,lbstp.plcc_payment_reference_nbr AS plcc_payment_reference_nbr
,NULL AS plcc_payment_line_object_cd
,lbstp.plcc_payment_tm AS plcc_payment_tm
,lbstp.plcc_payment_amt AS plcc_payment_amt 
,NULL AS p2e_ind
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'LB' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.lbca_lb_sales_transaction_header lbsth
LEFT JOIN  --added by Shareen 
(select 
chain_nbr
,store_nbr
,transaction_dt
,transaction_nbr
,register_nbr
,max(transaction_tm) as plcc_payment_tm
,max(payment_reference_nbr) as plcc_payment_reference_nbr
,sum(payment_amt) as plcc_payment_amt
from @STAGING_DATABASE.LBCA_LB_SALES_TRANSACTION_PAYMENT
group by 
chain_nbr
,store_nbr
,transaction_dt
,transaction_nbr
,register_nbr) lbstp
ON md5(concat (lbsth.chain_nbr,"|",lbsth.store_nbr,"|",lbsth.register_nbr,"|",lbsth.transaction_nbr,"|",lbsth.transaction_dt)) = md5(concat (lbstp.chain_nbr,"|",lbstp.store_nbr,"|",lbstp.register_nbr,"|",lbstp.transaction_nbr,"|",lbstp.transaction_dt))
where lbsth.chain_nbr IN (7) 
and lbsth.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd')
UNION
SELECT 
md5(concat('CA|',  --Nishanth
casth.store_nbr,"|",
casth.register_nbr,"|",
casth.transaction_dt,"|",
casth.transaction_nbr))AS transaction_key
,casth.store_nbr AS store_id
,casth.transaction_nbr AS transaction_num
,casth.register_nbr AS register_num
,casth.transaction_dt
,md5(concat ('CA|',casth.store_nbr)) as store_key  --Nishanth
,case when casth.transaction_tm = '' then NULL else casth.transaction_tm end AS transaction_tm
,case when casth.channel_cd = '' then NULL else casth.channel_cd end AS channel_cd
,case when casth.transaction_type_cd = '' then NULL else casth.transaction_type_cd end AS transaction_type_cd
,case when casth.entry_type_cd = '' then NULL else casth.entry_type_cd end AS transaction_entry_type_cd
,case when casth.cashier_nbr = '' then NULL else casth.cashier_nbr end AS cashier_id
,case when casth.manager_nbr = '' then NULL else casth.manager_nbr end AS store_manager_id
,case when casth.layaway_nbr = '' then NULL else casth.layaway_nbr end AS layaway_num
,case when trim(casth.multi_brand_transaction_ind)='Y' THEN 1
 when trim(casth.multi_brand_transaction_ind)='N' THEN 0
 when trim(casth.multi_brand_transaction_ind)='' then NULL end AS multi_brand_transaction_ind
,case when trim(casth.domestic_order_ind)='Y' THEN 1
 when trim(casth.domestic_order_ind)='N' THEN 0
 when trim(casth.domestic_order_ind)='' then NULL end AS domestic_order_ind
,case when trim(casth.address_capture_ind)='Y' THEN 1
 when trim(casth.address_capture_ind)='N' THEN 0
 when trim(casth.address_capture_ind)='' then NULL end AS address_capture_ind
,case when trim(casth.email_capture_ind)='Y' THEN 1
 when trim(casth.email_capture_ind)='N' THEN 0
 when trim(casth.email_capture_ind)='' then NULL end AS email_capture_ind 
,case when trim(casth.return_receipt_ind)='Y' THEN 1
 when trim(casth.return_receipt_ind)='N' THEN 0
 when trim(casth.return_receipt_ind)='' then NULL end AS return_receipt_ind 
,case when casth.order_nbr = '' then NULL else casth.order_nbr end AS order_num
,casth.order_received_dt AS order_receive_dt
,case when casth.order_received_tm = '' then NULL else casth.order_received_tm end AS order_receive_tm
,casth.transaction_void_ind
,case when casth.ship_to_store_nbr is not null and casth.ship_to_store_nbr<>0 then 
MD5(concat('CA|',COALESCE(casth.ship_to_store_nbr,''))) else NULL end AS ship_to_store_key --Nishanth
,case when casth.tender_reference_cd is not null and casth.tender_reference_cd <> 0 then "1" else "0" end AS membership_ind
,castp.plcc_payment_reference_nbr AS plcc_payment_reference_nbr
,NULL AS plcc_payment_line_object_cd
,castp.plcc_payment_tm AS plcc_payment_tm
,castp.plcc_payment_amt AS plcc_payment_amt 
,NULL AS p2e_ind 
,CURRENT_TIMESTAMP AS edl_create_tms  
,'TRANS' AS edl_create_job_nam  
,CURRENT_TIMESTAMP AS edl_last_update_tms  
,'TRANS' AS edl_last_update_job_nam  
,'CA' AS brand_cd  --Nishanth
FROM @STAGING_DATABASE.LBCA_CA_SALES_TRANSACTION_HEADER casth
LEFT JOIN --added by Shareen
(select 
chain_nbr
,store_nbr
,transaction_dt
,transaction_nbr
,register_nbr
,max(transaction_tm) as plcc_payment_tm
,max(payment_reference_nbr) as plcc_payment_reference_nbr
,sum(payment_amt) as plcc_payment_amt
from @STAGING_DATABASE.LBCA_CA_SALES_TRANSACTION_PAYMENT
group by 
chain_nbr
,store_nbr
,transaction_dt
,transaction_nbr
,register_nbr
) castp
ON md5(concat (casth.chain_nbr,"|",casth.store_nbr,"|",casth.register_nbr,"|",casth.transaction_nbr,"|",casth.transaction_dt)) = md5(concat (castp.chain_nbr,"|",castp.store_nbr,"|",castp.register_nbr,"|",castp.transaction_nbr,"|",castp.transaction_dt))
where casth.chain_nbr IN (4)
and casth.batch_id = from_unixtime(unix_timestamp(date_sub(current_date,1) , 'yyyy-MM-dd'), 'yyyyMMdd');
