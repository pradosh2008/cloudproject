CREATE TABLE  "plus_mart"."fact_transaction_detail"
WITH (
     format = 'Parquet'
    ,parquet_compression = 'SNAPPY'
    ,external_location = 's3://d1-asc-aadp-mart/plus/fact_transaction_detail/'
    ,partitioned_by = ARRAY['brand_cd','transaction_month']
)
AS
SELECT td.plus_sales_transaction_detail_key                    as transaction_detail_key
    ,th.plus_sales_transaction_header_key                      as transaction_key
    ,th.plus_selling_store_key                                 as selling_store_key
    ,td.transaction_dt                                         AS transaction_dt
    ,td.detail_seq_nbr                                         AS transaction_line_num
    ,td.plus_item_key                                          as item_key
    ,coalesce(sc.plus_vmci105_chain_cust_key
            ,th.plus_vmci105_chain_cust_key                            
            )                                                  as brand_customer_key
    ,coalesce(ot.plus_sales_transaction_header_key
             ,th.plus_sales_transaction_header_key                    
            )                                                  as Original_transaction_key
    ,case  when td.selling_store_nbr=6000
        then th.order_received_dt
        else th.transaction_dt
     end                                                       as order_dt
	,td.selling_store_nbr                                      as selling_store_num
    ,td.sku_nbr                                                as sku_num
    ,td.store_nbr                                              as store_num
    ,td.chain_nbr                                              as chain_num
    ,td.selling_chain_nbr                                      as selling_chain_num
    ,td.register_nbr                                           as register_num
    ,td.transaction_nbr                                        as transaction_num
    ,td.ship_from_store_nbr                                    as ship_from_store_num
    ,td.transaction_line_rtl
    ,case  when TRIM(td.SALES_RETURN_IND) = 'Y'
        then 1
        when TRIM(td.SALES_RETURN_IND)='N'
        THEN 0
        when td.SALES_RETURN_IND=''
        then NULL
     end                                           AS return_ind
    ,td.original_chain_nbr                         as original_chain_num
    ,td.original_store_nbr                         as original_store_num 
    ,td.original_register_nbr                      as original_register_num 
    ,td.original_transaction_nbr                   as original_transaction_num 
    ,td.original_transaction_dt                    AS original_transaction_dt
    ,td.detail_seq_nbr                             AS original_transaction_line_num
    ,td.line_action_cd
    ,td.line_object_type_cd
    ,td.line_object_cd
    ,case   when td.line_void_ind ='Y'
        then 1
        else 0
     end                                           as line_void_ind
    ,case   when td.scanned_ind ='Y'
        then 1
        else 0
     end                                           as scanned_ind
    ,case   when td.gift_card_ind ='Y'
        then 1
        else 0
     end                                           as gift_card_ind
    ,case   when td.taxable_ind ='Y'
        then 1
        else 0
     end                                           as taxable_ind
    ,case   when td.style_locator_ind ='Y'
        then 1
        else 0
     end                                           as style_locator_ind
    ,td.permanent_markdown_type_cd
    ,td.return_reason_cd
    ,td.return_disposition_cd
	
	
	,cast(td.sales_qty as bigint) as sales_quantity_num
	,td.net_sales_rtl as net_sales_retail_amt
	,cast(td.return_qty as bigint) as return_quantity_num
	,td.return_rtl as return_retail_amt
	
    ,case   when td.sales_layaway_ind ='Y'
        then 1
        else 0
     end                                           as layaway_ind

        ,td.transaction_line_cst                   as net_unit_cogs_amt  
    ,td.transaction_line_cst                       as sku_cost_amt
    ,cast(td.transaction_line_qty as bigint)        as sku_quantity_num


    ,td.sales_discount_markdown_rtl as sales_discount_markdown_retail_amt
	,td.return_discount_markdown_rtl as return_discount_markdown_retail_amt



    ,td.customer_paid_tax_amt                      as customer_paid_tax_amt
    ,td.company_owed_tax_amt                       as company_owed_tax_amt
    ,td.commission_amt                             as commision_amt
    ,td.current_rtl                                as current_retail_amt
    ,td.original_unit_rtl                          as original_unit_retail_amt
    ,cast(td.primary_salesperson_nbr as varchar)    as primary_salesperson_id
    ,cast(td.secondary_salesperson_nbr as varchar)  as secondary_salesperson_id
    ,td.ret_primary_sales_person_nbr               as original_primary_salesperson_id
    ,td.ret_secondary_sales_person_nbr             as original_secondary_salesperson_id
    ,cast(td.upc_nbr as bigint)                     as upc_num
    ,th.plus_selling_store_key                     as cross_brand_store_key
    ,case   when td.overseas_item_ind ='Y'
        then 1
        else 0
     end                                             as overseas_item_ind
    ,td.record_seq_nbr     as record_seq_num
    ,td.merchandise_salesperson_nbr     as merchandise_salesperson_num
    ,td.perm_markdown_type_cd     as perm_markdown_type_cd
    ,td.sales_return_ind     as sales_return_ind
    ,td.selection_chain_nbr     as selection_chain_num
    ,td.selection_store_nbr     as selection_store_num
    ,td.discount_markdown_rtl     as discount_markdown_rtl_amt
    ,td.gross_profit_net_sales_rtl     as gross_profit_net_sales_rtl_amt
    ,td.gross_profit_prodtv_sls_rtl     as gross_profit_prodtv_sls_rtl_amt
    ,td.gross_profit_return_rtl     as gross_profit_return_rtl_amt
    ,td.gross_sales_cst     as gross_sales_cst_amt
    ,cast(td.gross_sales_qty as bigint)    as gross_sales_quantity_num
    ,td.gross_sales_rtl     as gross_sales_rtl_amt
    ,td.net_sales_cst     as net_sales_cst_amt
    ,td.net_sales_current_rtl     as net_sales_current_rtl_amt
    ,td.net_sales_original_rtl     as net_sales_original_rtl_amt
    ,cast(td.net_sales_qty as bigint)    as net_sales_quantity_num
    ,td.productive_sales_cst     as productive_sales_cst_amt
    ,td.productive_sales_current_rtl     as productive_sales_current_rtl_amt
    ,td.productive_sales_original_rtl     as productive_sales_original_rtl_amt
    ,cast(td.productive_sales_qty as bigint)     as productive_sales_quantity_num
    ,td.productive_sales_register_rtl     as productive_sales_register_rtl_amt
    ,td.productive_sales_rtl     as productive_sales_rtl_amt
    ,td.return_cst     as return_cst_amt
    ,td.return_current_rtl     as return_current_rtl_amt
    ,td.return_discount_markdown_rtl     as return_discount_markdown_rtl_amt
    ,td.return_original_rtl     as return_original_rtl_amt
    ,td.return_promo_markdown_rtl     as return_promo_markdown_rtl_amt
    ,td.sales_cst     as sales_cst_amt
    ,td.sales_discount_markdown_rtl     as sales_discount_markdown_rtl_amt
    ,td.sales_promo_markdown_rtl     as sales_promo_markdown_rtl_amt
    ,td.sales_rtl     as sales_rtl_amt
    ,td.style_locator_cst     as style_locator_cst_amt
    ,cast(td.style_locator_qty as bigint)    as style_locator_quantity_num
    ,td.style_locator_rtl     as style_locator_rtl_amt
    ,td.tax_amt     as tax_amt
    ,td.extract_ts     as extract_tms
    ,td.create_dt     as create_dt
    ,td.last_update_ts     as last_update_tms
    ,td.brand_cd                                                   as brand_cd
    ,td.transaction_month                                          as transaction_month
FROM "plus_foundation"."plus_sales_transaction_detail" td
left join "plus_foundation"."plus_sales_transaction_header" th
   on th.plus_sales_transaction_header_key = td.plus_sales_transaction_header_key
   and th.brand_cd = td.brand_cd
   and th.selling_chain_nbr = td.selling_chain_nbr
   and th.transaction_month = td.transaction_month
left join "plus_foundation"."plus_sales_transaction_header" ot
    on  ot.selling_chain_nbr = td.original_chain_nbr
    and ot.selling_store_nbr = td.original_store_nbr
    and ot.register_nbr      = td.original_register_nbr
    and ot.transaction_dt    = td.original_transaction_dt
    and ot.transaction_nbr   = td.original_transaction_nbr
    and ot.brand_cd          = td.brand_cd
left join "plus_foundation_dev"."plus_sales_customer" sc
   on sc.plus_sales_transaction_header_key = td.plus_sales_transaction_header_key
   and element_at(split(sc.source_file_nam,'_'),1) = td.brand_cd
   and sc.selling_chain_nbr = td.selling_chain_nbr
   and sc.transaction_month = td.transaction_month
;
