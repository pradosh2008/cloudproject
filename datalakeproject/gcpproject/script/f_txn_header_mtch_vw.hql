set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE `ascena_analytic_mart.f_txn_header_mtch`(
  `txn_id` decimal(10,0),
  `txn_nbr` string,
  `txn_source_cd` string,
  `brand_cd` string,
  `store_nbr` decimal(10,0),
  `register_nbr` decimal(5,0),
  `txn_dt` timestamp,
  `txn_time` string,
  `txn_type_cd` string,
  `txn_channel_cd` string,
  `txn_status_cd` string,
  `orig_web_txn_id` decimal(10,0),
  `bill_indiv_id` decimal(13,0),
  `bill_hh_id` decimal(13,0),
  `bill_addr_id` string,
  `bill_usps_geo_id` decimal(10,0),
  `alloc_indiv_id` decimal(13,0),
  `alloc_hh_id` decimal(13,0),
  `alloc_addr_id` string,
  `email_id` decimal(10,0),
  `rm_method_cd` string,
  `rm_vendor_cd` string,
  `rm_dt` timestamp,
  `cashier_emp_nbr` string,
  `emp_purch_ind` decimal(1,0),
  `zip_capture_ind` decimal(1,0),
  `email_capture_ind` decimal(1,0),
  `new_email_ind` decimal(1,0),
  `bankcard_ind` decimal(1,0),
  `plcc_ind` decimal(1,0),
  `cash_ind` decimal(1,0),
  `gc_ind` decimal(1,0),
  `receipt_delivery_method_cd` decimal(1,0),
  `email_capture_type_cd` decimal(1,0),
  `email_preference_ind` decimal(1,0),
  `email_capture_loc_ind` decimal(1,0),
  `international_ind` decimal(1,0),
  `ip_address` string,
  `txn_gross_amt` float,
  `txn_disc_amt` float,
  `txn_adj_amt` float,
  `txn_net_amt` float,
  `txn_sh_amt` float,
  `txn_tax_amt` float,
  `txn_gc_amt` float,
  `txn_ret_net_amt` float,
  `txn_ret_sh_amt` float,
  `txn_ret_tax_amt` float,
  `txn_ret_oth_amt` float,
  `txn_ret_fee_amt` float,
  `txn_sold_qty` decimal(6,0),
  `txn_ret_qty` decimal(6,0),
  `txn_cogs` float,
  `txn_qty` decimal(6,0),
  `primary_tndr_type_cd` string,
  `txn_ret_cogs` float,
  `txn_net_net_amount` float,
  `web_rtn_at_pos_ind` decimal(1,0),
  `ipad_txn_ind` decimal(1,0),
  `ipad_cashier_emp_nbr` string,
  `ipad_store_nbr` decimal(10,0),
  `originoforder` string,
  `source_keycode` string,
  `wallet_ind` decimal(1,0),
  `partner_cd` string,
  `bopis_item_adj_cd` string
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

INSERT OVERWRITE TABLE ascena_analytic_mart.f_txn_header_mtch
select txn_id
,txn_nbr
,txn_source_cd
,brand_cd
,store_nbr
,register_nbr
,txn_dt
,txn_time
,txn_type_cd
,txn_channel_cd
,txn_status_cd
,orig_web_txn_id
,bill_indiv_id
,bill_hh_id
,bill_addr_id
,bill_usps_geo_id
,alloc_indiv_id
,alloc_hh_id
,alloc_addr_id
,email_id
,rm_method_cd
,rm_vendor_cd
,rm_dt
,cashier_emp_nbr
,emp_purch_ind
,zip_capture_ind
,email_capture_ind
,new_email_ind
,bankcard_ind
,plcc_ind
,cash_ind
,gc_ind
,receipt_delivery_method_cd
,email_capture_type_cd
,email_preference_ind
,email_capture_loc_ind
,international_ind
,ip_address
,cast (txn_gross_amt as float) as txn_gross_amt
,cast (txn_disc_amt as float) as txn_disc_amt
,cast (txn_adj_amt as float) as txn_adj_amt
,cast (txn_net_amt as float) as txn_net_amt
,cast (txn_sh_amt as float) as txn_sh_amt
,cast (txn_tax_amt as float) as txn_tax_amt 
,cast (txn_gc_amt as float) as txn_gc_amt
,cast (txn_ret_net_amt as float) as txn_ret_net_amt
,cast (txn_ret_sh_amt as float) as txn_ret_sh_amt
,cast (txn_ret_tax_amt as float) as txn_ret_tax_amt
,cast (txn_ret_oth_amt as float) as txn_ret_oth_amt
,cast (txn_ret_fee_amt as float) as txn_ret_fee_amt
,cast (txn_sold_qty as float) as txn_sold_qty
,cast (txn_ret_qty as float) as txn_ret_qty
,cast (txn_cogs as float) as txn_cogs
,cast (txn_qty as float) as txn_qty
,primary_tndr_type_cd
,cast(txn_ret_cogs as float) as txn_ret_cogs
,cast(txn_net_net_amount as float) as txn_net_net_amount
,web_rtn_at_pos_ind
,ipad_txn_ind
,ipad_cashier_emp_nbr
,ipad_store_nbr
,originoforder
,source_keycode
,wallet_ind
,partner_cd
,bopis_item_adj_cd
from ascena_staging.f_txn_header_mtch_vw;
