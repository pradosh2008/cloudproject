#!/usr/bin/bash

. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh

#creating the LB landing table
#bq load --replace=true --source_format=CSV --skip_leading_rows=1 --field_delimiter="|" \
#        --schema=/home/svc_gcpintegnp/schema/edl_landing/lbca_sales_transaction_detail.json \
#        edl_landing.lb_sales_transaction_detail_hist  \
#        "gs://p-asna-datasink-003/plus/lb_sales_transaction_detail_EDW_hist/lb_sales_transaction_detail20200424.txt.gz"
#rc_check $? "Load edl_landing LB Complted"
#
##creating the CA landing table
#bq load --replace=true --source_format=CSV --skip_leading_rows=1 --field_delimiter="|" \
#        --schema=/home/svc_gcpintegnp/schema/edl_landing/lbca_sales_transaction_detail.json \
#        edl_landing.ca_sales_transaction_detail_hist  \
#       "gs://p-asna-datasink-003/plus/lb_sales_transaction_detail_EDW_hist/ca_sales_transaction_detail20200424.txt.gz"
#

#loading the temporary table in staging ( it has both new and updated records)
bq query --replace=true --max_rows 1 --allow_large_results --use_legacy_sql=false \
         --destination_table edl_stage.lb_sales_transaction_detail_hist_new <<!
SELECT 
#stg.line_action_cd
#,stg.line_object_type_cd
#,stg.line_object_cd
#,stg.line_void_ind
#,stg.permanent_markdown_type_cd
#,stg.discount_markdown_rtl
#,stg.customer_paid_tax_amt
#,stg.company_owed_tax_amt
#,stg.return_reason_cd
#,stg.commission_amt
#,stg.current_rtl
#,stg.original_unit_rtl
#,stg.scanned_ind
#,stg.gift_card_ind
#,stg.taxable_ind
#,stg.style_locator_ind
#,stg.primary_salesperson_nbr
#,stg.secondary_salesperson_nbr
#,stg.sales_return_ind
#,stg.original_store_nbr  
#,stg.original_register_nbr
#,stg.original_transaction_dt
#,stg.original_transaction_nbr
#,stg.ret_primary_sales_person_nbr
#,stg.ret_secondary_sales_person_nbr
#,stg.return_disposition_cd
#,stg.sales_layaway_ind
#,stg.upc_nbr
#,stg.selection_store_nbr
#,stg.overseas_item_ind



stg.selling_chain_nbr
,stg.selling_store_nbr
,stg.chain_nbr
,stg.store_nbr
,stg.transaction_dt
,stg.register_nbr
,stg.transaction_nbr
,stg.record_seq_nbr
,stg.detail_seq_nbr
,stg.sku_nbr
,stg.transaction_line_cst
,stg.transaction_line_qty
,stg.transaction_line_rtl


#,stg.merchandise_salesperson_nbr
#,stg.selection_chain_nbr
#,stg.ship_from_store_nbr
#,stg.return_qty
#,stg.return_cst
#,stg.return_rtl
#,stg.sales_qty
#,stg.sales_cst
#,stg.sales_rtl
#,stg.sales_discount_markdown_rtl
#,stg.sales_promo_markdown_rtl
#,stg.return_discount_markdown_rtl
#,stg.return_promo_markdown_rtl
#,stg.productive_sales_qty
#,stg.productive_sales_cst
#,stg.productive_sales_rtl
#,stg.productive_sales_original_rtl
#,stg.productive_sales_current_rtl
#,stg.productive_sales_register_rtl
#,stg.gross_sales_qty
#,stg.gross_sales_cst
#,stg.gross_sales_rtl
#,stg.net_sales_qty
#,stg.net_sales_cst
#,stg.net_sales_rtl
#,stg.style_locator_qty
#,stg.style_locator_cst
#,stg.style_locator_rtl
#,stg.net_sales_original_rtl
#,stg.net_sales_current_rtl
#,stg.return_original_rtl
#,stg.return_current_rtl
#,stg.gross_profit_prodtv_sls_rtl
#,stg.gross_profit_return_rtl
#,stg.gross_profit_net_sales_rtl
#,stg.tax_amt
#,stg.perm_markdown_type_cd
#,stg.create_dt
#,stg.last_update_ts
#,stg.original_chain_nbr
#,stg.extract_ts
from (SELECT lbca.*
,ROW_NUMBER() OVER (partition by lbca.selling_chain_nbr,lbca.selling_store_nbr, lbca.register_nbr, lbca.transaction_dt, lbca.transaction_nbr,lbca.detail_seq_nbr
order by lbca.extract_ts desc) as row_num
from
(SELECT 
#c.line_action_cd
#,c.line_object_type_cd
#,c.line_object_cd
#,c.line_void_ind
#,c.permanent_markdown_type_cd
#,CAST(TRIM(c.discount_markdown_rtl) AS NUMERIC) AS discount_markdown_rtl
#,CASE WHEN COALESCE(TRIM(c.customer_paid_tax_amt), '') = '' THEN 0.00 ELSE CAST(TRIM(c.customer_paid_tax_amt) AS NUMERIC) END AS customer_paid_tax_amt
#,CAST(TRIM(c.company_owed_tax_amt) AS NUMERIC) AS company_owed_tax_amt
#,c.return_reason_cd
#,CAST(TRIM(c.commission_amt) AS NUMERIC) AS commission_amt
#,CASE WHEN COALESCE(TRIM(c.current_rtl), '') = '' THEN 0.00 ELSE CAST(TRIM(c.current_rtl) AS NUMERIC) END AS current_rtl
#,CAST(TRIM(c.original_unit_rtl) AS NUMERIC) AS original_unit_rtl
#,c.scanned_ind
#,c.gift_card_ind
#,c.taxable_ind
#,c.style_locator_ind
#,CAST(TRIM(c.primary_salesperson_nbr) AS NUMERIC) AS primary_salesperson_nbr
#,CAST(TRIM(c.secondary_salesperson_nbr) AS NUMERIC) AS secondary_salesperson_nbr
#,c.sales_return_ind
#,CASE WHEN COALESCE(TRIM(c.original_store_nbr), '') = '' THEN 0.00 ELSE CAST(TRIM(c.original_store_nbr) AS NUMERIC) END AS original_store_nbr
#,CASE WHEN COALESCE(TRIM(c.original_register_nbr), '') = '' THEN 0 ELSE CAST(TRIM(c.original_register_nbr) AS INT64) END AS original_register_nbr
#,case when TRIM(c.original_transaction_dt)='' then null
# else PARSE_DATE("%Y-%m-%d",TRIM(c.original_transaction_dt)) end as original_transaction_dt
#,CASE WHEN COALESCE(TRIM(c.original_transaction_nbr), '') = '' THEN 0 ELSE CAST(TRIM(c.original_transaction_nbr) AS INT64) END AS original_transaction_nbr
#,c.ret_primary_sales_person_nbr
#,c.ret_secondary_sales_person_nbr
#,c.return_disposition_cd
#,c.sales_layaway_ind
#,c.upc_nbr
#,CAST(TRIM(c.selection_store_nbr) AS NUMERIC) AS selection_store_nbr
#,c.overseas_item_ind
CAST(TRIM(c.selling_chain_nbr) AS INT64) AS selling_chain_nbr
,CAST(TRIM(c.selling_store_nbr) AS NUMERIC) AS selling_store_nbr
,CAST(TRIM(c.chain_nbr) AS INT64) AS chain_nbr
,CAST(TRIM(c.store_nbr) AS NUMERIC) AS store_nbr
,case when TRIM(c.transaction_dt)='' then null
 else PARSE_DATE("%Y-%m-%d",TRIM(c.transaction_dt)) end as transaction_dt
,CAST(TRIM(c.register_nbr) AS INT64) AS register_nbr
,CAST(TRIM(c.transaction_nbr) AS INT64) AS transaction_nbr
,CAST(TRIM(c.record_seq_nbr) AS INT64) AS record_seq_nbr
,CAST(TRIM(c.detail_seq_nbr) AS INT64) AS detail_seq_nbr
,c.sku_nbr
,CAST(TRIM(c.transaction_line_cst) AS NUMERIC) AS transaction_line_cst
,CAST(TRIM(c.transaction_line_qty) AS NUMERIC) AS transaction_line_qty
,CAST(TRIM(c.transaction_line_rtl) AS NUMERIC) AS transaction_line_rtl
#,CAST(TRIM(c.merchandise_salesperson_nbr) AS NUMERIC) AS merchandise_salesperson_nbr
#,CASE WHEN COALESCE(TRIM(c.selection_chain_nbr), '') = '' THEN 0 ELSE CAST(TRIM(c.selection_chain_nbr) AS INT64) END AS selection_chain_nbr
#,CASE WHEN COALESCE(TRIM(c.ship_from_store_nbr), '') = '' THEN 0.00 ELSE CAST(TRIM(c.ship_from_store_nbr) AS NUMERIC) END AS ship_from_store_nbr
#,CAST(TRIM(c.return_qty) AS NUMERIC) AS return_qty
#,CAST(TRIM(c.return_cst) AS NUMERIC) AS return_cst
#,CAST(TRIM(c.return_rtl) AS NUMERIC) AS return_rtl
#,CAST(TRIM(c.sales_qty) AS NUMERIC) AS sales_qty
#,CAST(TRIM(c.sales_cst) AS NUMERIC) AS sales_cst
#,CAST(TRIM(c.sales_rtl) AS NUMERIC) AS sales_rtl
#,CAST(TRIM(c.sales_discount_markdown_rtl) AS NUMERIC) AS sales_discount_markdown_rtl
#,c.sales_promo_markdown_rtl
#,CAST(TRIM(c.return_discount_markdown_rtl) AS NUMERIC) AS return_discount_markdown_rtl
#,c.return_promo_markdown_rtl
#,CAST(TRIM(c.productive_sales_qty) AS NUMERIC) AS productive_sales_qty
#,CAST(TRIM(c.productive_sales_cst) AS NUMERIC) AS productive_sales_cst
#,CAST(TRIM(c.productive_sales_rtl) AS NUMERIC) AS productive_sales_rtl
#,CAST(TRIM(c.productive_sales_original_rtl) AS NUMERIC) AS productive_sales_original_rtl
#,CASE WHEN COALESCE(TRIM(c.productive_sales_current_rtl), '') = '' THEN 0.00 ELSE CAST(TRIM(c.productive_sales_current_rtl) AS NUMERIC) END AS productive_sales_current_rtl
#,CAST(TRIM(c.productive_sales_register_rtl) AS NUMERIC) AS productive_sales_register_rtl
#,CAST(TRIM(c.gross_sales_qty) AS NUMERIC) AS gross_sales_qty
#,CAST(TRIM(c.gross_sales_cst) AS NUMERIC) AS gross_sales_cst
#,CAST(TRIM(c.gross_sales_rtl) AS NUMERIC) AS gross_sales_rtl
#,CAST(TRIM(c.net_sales_qty) AS NUMERIC) AS net_sales_qty
#,CAST(TRIM(c.net_sales_cst) AS NUMERIC) AS net_sales_cst
#,CAST(TRIM(c.net_sales_rtl) AS NUMERIC) AS net_sales_rtl
#,CAST(TRIM(c.style_locator_qty) AS NUMERIC) AS style_locator_qty
#,CAST(TRIM(c.style_locator_cst) AS NUMERIC) AS style_locator_cst
#,CAST(TRIM(c.style_locator_rtl) AS NUMERIC) AS style_locator_rtl
#,CAST(TRIM(c.net_sales_original_rtl) AS NUMERIC) AS net_sales_original_rtl
#,CASE WHEN COALESCE(TRIM(c.net_sales_current_rtl), '') = '' THEN 0.00 ELSE CAST(TRIM(c.net_sales_current_rtl) AS NUMERIC) END AS net_sales_current_rtl
#,CAST(TRIM(c.return_original_rtl) AS NUMERIC) AS return_original_rtl
#,CASE WHEN COALESCE(TRIM(c.net_sales_current_rtl), '') = '' THEN 0.00 ELSE CAST(TRIM(c.return_current_rtl) AS NUMERIC) END AS return_current_rtl
#,CAST(TRIM(c.gross_profit_prodtv_sls_rtl) AS NUMERIC) AS gross_profit_prodtv_sls_rtl
#,CAST(TRIM(c.gross_profit_return_rtl) AS NUMERIC) AS gross_profit_return_rtl
#,CAST(TRIM(c.gross_profit_net_sales_rtl) AS NUMERIC) AS gross_profit_net_sales_rtl
#,CASE WHEN COALESCE(TRIM(c.tax_amt), '') = '' THEN 0.00 ELSE CAST(TRIM(c.tax_amt) AS NUMERIC) END AS tax_amt
#,c.perm_markdown_type_cd
#,case when TRIM(c.create_dt)='' then null
# else PARSE_DATE("%y/%m/%d",TRIM(c.create_dt))end as create_dt
#,case when TRIM(c.last_update_ts)='' then null
# else PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",c.last_update_ts,"America/New_York")end as last_update_ts
#,CASE WHEN COALESCE(TRIM(c.original_chain_nbr),'') = '' THEN 0 ELSE CAST(TRIM(c.original_chain_nbr) AS INT64) END AS original_chain_nbr
,case when TRIM(c.extract_ts)=''then null
 else PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",c.extract_ts,"America/New_York") end as extract_ts
 from edl_landing.lb_sales_transaction_detail_hist c
 UNION ALL
 SELECT 
#c.line_action_cd
#,c.line_object_type_cd
#,c.line_object_cd
#,c.line_void_ind
#,c.permanent_markdown_type_cd
#,CAST(TRIM(c.discount_markdown_rtl) AS NUMERIC) AS discount_markdown_rtl
#,CASE WHEN COALESCE(TRIM(c.customer_paid_tax_amt), '') = '' THEN 0.00 ELSE CAST(TRIM(c.customer_paid_tax_amt) AS NUMERIC) END AS customer_paid_tax_amt
#,CAST(TRIM(c.company_owed_tax_amt) AS NUMERIC) AS company_owed_tax_amt
#,c.return_reason_cd
#,CAST(TRIM(c.commission_amt) AS NUMERIC) AS commission_amt
#,CASE WHEN COALESCE(TRIM(c.current_rtl), '') = '' THEN 0.00 ELSE CAST(TRIM(c.current_rtl) AS NUMERIC) END AS current_rtl
#,CAST(TRIM(c.original_unit_rtl) AS NUMERIC) AS original_unit_rtl
#,c.scanned_ind
#,c.gift_card_ind
#,c.taxable_ind
#,c.style_locator_ind
#,CAST(TRIM(c.primary_salesperson_nbr) AS NUMERIC) AS primary_salesperson_nbr
#,CAST(TRIM(c.secondary_salesperson_nbr) AS NUMERIC) AS secondary_salesperson_nbr
#,c.sales_return_ind
#,CASE WHEN COALESCE(TRIM(c.original_store_nbr), '') = '' THEN 0.00 ELSE CAST(TRIM(c.original_store_nbr) AS NUMERIC) END AS original_store_nbr
#,CASE WHEN COALESCE(TRIM(c.original_register_nbr), '') = '' THEN 0 ELSE CAST(TRIM(c.original_register_nbr) AS INT64) END AS original_register_nbr
#,case when TRIM(c.original_transaction_dt)='' then null
# else PARSE_DATE("%Y-%m-%d",TRIM(c.original_transaction_dt)) end as original_transaction_dt
#,CASE WHEN COALESCE(TRIM(c.original_transaction_nbr), '') = '' THEN 0 ELSE CAST(TRIM(c.original_transaction_nbr) AS INT64) END AS original_transaction_nbr
#,c.ret_primary_sales_person_nbr
#,c.ret_secondary_sales_person_nbr
#,c.return_disposition_cd
#,c.sales_layaway_ind
#,c.upc_nbr
#,CAST(TRIM(c.selection_store_nbr) AS NUMERIC) AS selection_store_nbr
#,c.overseas_item_ind
CAST(TRIM(c.selling_chain_nbr) AS INT64) AS selling_chain_nbr
,CAST(TRIM(c.selling_store_nbr) AS NUMERIC) AS selling_store_nbr
,CAST(TRIM(c.chain_nbr) AS INT64) AS chain_nbr
,CAST(TRIM(c.store_nbr) AS NUMERIC) AS store_nbr
,case when TRIM(c.transaction_dt)='' then null
 else PARSE_DATE("%Y-%m-%d",TRIM(c.transaction_dt)) end as transaction_dt
,CAST(TRIM(c.register_nbr) AS INT64) AS register_nbr
,CAST(TRIM(c.transaction_nbr) AS INT64) AS transaction_nbr
,CAST(TRIM(c.record_seq_nbr) AS INT64) AS record_seq_nbr
,CAST(TRIM(c.detail_seq_nbr) AS INT64) AS detail_seq_nbr
,c.sku_nbr
,CAST(TRIM(c.transaction_line_cst) AS NUMERIC) AS transaction_line_cst
,CAST(TRIM(c.transaction_line_qty) AS NUMERIC) AS transaction_line_qty
,CAST(TRIM(c.transaction_line_rtl) AS NUMERIC) AS transaction_line_rtl
#,CAST(TRIM(c.merchandise_salesperson_nbr) AS NUMERIC) AS merchandise_salesperson_nbr
#,CASE WHEN COALESCE(TRIM(c.selection_chain_nbr), '') = '' THEN 0 ELSE CAST(TRIM(c.selection_chain_nbr) AS INT64) END AS selection_chain_nbr
#,CASE WHEN COALESCE(TRIM(c.ship_from_store_nbr), '') = '' THEN 0.00 ELSE CAST(TRIM(c.ship_from_store_nbr) AS NUMERIC) END AS ship_from_store_nbr
#,CAST(TRIM(c.return_qty) AS NUMERIC) AS return_qty
#,CAST(TRIM(c.return_cst) AS NUMERIC) AS return_cst
#,CAST(TRIM(c.return_rtl) AS NUMERIC) AS return_rtl
#,CAST(TRIM(c.sales_qty) AS NUMERIC) AS sales_qty
#,CAST(TRIM(c.sales_cst) AS NUMERIC) AS sales_cst
#,CAST(TRIM(c.sales_rtl) AS NUMERIC) AS sales_rtl
#,CAST(TRIM(c.sales_discount_markdown_rtl) AS NUMERIC) AS sales_discount_markdown_rtl
#,c.sales_promo_markdown_rtl
#,CAST(TRIM(c.return_discount_markdown_rtl) AS NUMERIC) AS return_discount_markdown_rtl
#,c.return_promo_markdown_rtl
#,CAST(TRIM(c.productive_sales_qty) AS NUMERIC) AS productive_sales_qty
#,CAST(TRIM(c.productive_sales_cst) AS NUMERIC) AS productive_sales_cst
#,CAST(TRIM(c.productive_sales_rtl) AS NUMERIC) AS productive_sales_rtl
#,CAST(TRIM(c.productive_sales_original_rtl) AS NUMERIC) AS productive_sales_original_rtl
#,CASE WHEN COALESCE(TRIM(c.productive_sales_current_rtl), '') = '' THEN 0.00 ELSE CAST(TRIM(c.productive_sales_current_rtl) AS NUMERIC) END AS productive_sales_current_rtl
#,CAST(TRIM(c.productive_sales_register_rtl) AS NUMERIC) AS productive_sales_register_rtl
#,CAST(TRIM(c.gross_sales_qty) AS NUMERIC) AS gross_sales_qty
#,CAST(TRIM(c.gross_sales_cst) AS NUMERIC) AS gross_sales_cst
#,CAST(TRIM(c.gross_sales_rtl) AS NUMERIC) AS gross_sales_rtl
#,CAST(TRIM(c.net_sales_qty) AS NUMERIC) AS net_sales_qty
#,CAST(TRIM(c.net_sales_cst) AS NUMERIC) AS net_sales_cst
#,CAST(TRIM(c.net_sales_rtl) AS NUMERIC) AS net_sales_rtl
#,CAST(TRIM(c.style_locator_qty) AS NUMERIC) AS style_locator_qty
#,CAST(TRIM(c.style_locator_cst) AS NUMERIC) AS style_locator_cst
#,CAST(TRIM(c.style_locator_rtl) AS NUMERIC) AS style_locator_rtl
#,CAST(TRIM(c.net_sales_original_rtl) AS NUMERIC) AS net_sales_original_rtl
#,CASE WHEN COALESCE(TRIM(c.net_sales_current_rtl), '') = '' THEN 0.00 ELSE CAST(TRIM(c.net_sales_current_rtl) AS NUMERIC) END AS net_sales_current_rtl
#,CAST(TRIM(c.return_original_rtl) AS NUMERIC) AS return_original_rtl
#,CASE WHEN COALESCE(TRIM(c.net_sales_current_rtl), '') = '' THEN 0.00 ELSE CAST(TRIM(c.return_current_rtl) AS NUMERIC) END AS return_current_rtl
#,CAST(TRIM(c.gross_profit_prodtv_sls_rtl) AS NUMERIC) AS gross_profit_prodtv_sls_rtl
#,CAST(TRIM(c.gross_profit_return_rtl) AS NUMERIC) AS gross_profit_return_rtl
#,CAST(TRIM(c.gross_profit_net_sales_rtl) AS NUMERIC) AS gross_profit_net_sales_rtl
#,CASE WHEN COALESCE(TRIM(c.tax_amt), '') = '' THEN 0.00 ELSE CAST(TRIM(c.tax_amt) AS NUMERIC) END AS tax_amt
#,c.perm_markdown_type_cd
#,case when TRIM(c.create_dt)='' then null
# else PARSE_DATE("%y/%m/%d",TRIM(c.create_dt))end as create_dt
#,case when TRIM(c.last_update_ts)='' then null
# else PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",c.last_update_ts,"America/New_York")end as last_update_ts
#,CASE WHEN COALESCE(TRIM(c.original_chain_nbr),'') = '' THEN 0 ELSE CAST(TRIM(c.original_chain_nbr) AS INT64) END AS original_chain_nbr
,case when TRIM(c.extract_ts)=''then null
 else PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",c.extract_ts,"America/New_York") end as extract_ts
 from edl_landing.ca_sales_transaction_detail_hist c
 ) lbca
 )stg
 where stg.row_num = 1
!
 
#append the old records into the temporary table
bq query  --max_rows 1 --allow_large_results --append_table --destination_table edl_stage.lb_sales_transaction_detail_hist_new --use_legacy_sql=false <<!
select c.*
from edl_stage.plus_sales_transaction_detaill_hist c
left join edl_stage.lb_sales_transaction_detail_hist_new w
     on w.selling_chain_nbr=c.selling_chain_nbr
    and w.selling_store_nbr=c.selling_store_nbr
    and w.register_nbr=c.register_nbr
    and w.transaction_dt=c.transaction_dt
    and w.transaction_nbr=c.transaction_nbr
    and w.detail_seq_nbr=c.detail_seq_nbr
where w.detail_seq_nbr is null
!
rc_check $? "append legacy records into the temp table"

#cleansing and archival
bq cp --force edl_stage.plus_sales_transaction_detaill_hist edl_archive.plus_sales_transaction_detaill_hist
rc_check $? "archive copy"
bq cp --force edl_stage.lb_sales_transaction_detail_hist_new edl_stage.plus_sales_transaction_detaill_hist
rc_check $? "replace the temp table as the stage table"
bq rm --force edl_stage.lb_sales_transaction_detail_hist_new
rc_check $? "drop the temp table"

#archive_bucket_files "gs://${default_bucket}/plus/edw/lb_sales_transaction_detail*.txt.gz*"
#archive_bucket_files "gs://${default_bucket}/plus/edw/ca_sales_transaction_detail*.txt.gz*"