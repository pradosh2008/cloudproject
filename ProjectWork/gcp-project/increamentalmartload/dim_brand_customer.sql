--/*********************************************************************
--* 
--* Script Name: DIM_BRAND_CUSTOMER table creation and data population
--*
--**********************************************************************/


set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;

--brand_customer_key: md5(concat('JUS',c.individual_id)) 
drop table ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER ;
CREATE table IF NOT EXISTS ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER (
 brand_customer_key     char(32)
,brand_cd               string
,individual_id bigint
,household_id BIGINT
,residential_id INT
,child_IND TINYINT
,deceased_IND TINYINT
,bill_to_ship_to_CD  string
,loyalty_num STRING
,plcc_status_CD  string
,plcc_card_type_cd  string
,plcc_last_purchase_dt DATE
,new_to_file_reactivated_CD  string
,loyalty_store_num INT
,trans_loyalty_store_12_mo_cnt INT      --need to confirm this is a count measure
,net_sales_lifetime_limited_too_amt DECIMAL(10,6)
,do_not_contact_IND TINYINT
,do_not_statement_insert_IND TINYINT
,do_not_sell_name_IND TINYINT
,do_not_list_share_IND TINYINT
,direct_mail_preference_CD  string
,email_preference_CD  string
,text_message_preference_CD  string
,mass_individual_IND TINYINT
,corporate_individual_id INT
,create_tms TIMESTAMP
,create_source_CD  string
,create_job_id INT
,name_address_updt_src_CD  string
,last_update_tms TIMESTAMP
,last_update_source_CD  string
,last_update_job_id INT
,coppa_IND TINYINT
,profanity_IND TINYINT
--,ads_promotability_IND TINYINT
--,ads_email_preference_CD  string
--,ads_direct_mail_pref_CD  string
--,ads_do_not_statement_ins_IND TINYINT
--,ads_do_not_telemarket_IND TINYINT
--,ads_do_not_sell_name_IND TINYINT
--,ads_return_mail_IND TINYINT
--,ads_contact_pref_CD  string
--,ads_spam_pref_CD  string
--,open_to_buy INT
,batch_id BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

-- BRAND_CUSTOMER_KEY is the primary key
INSERT OVERWRITE TABLE ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER
SELECT md5(concat('JUS|',coalesce(c.individual_id,''))) as brand_customer_key 
    ,'JUS' as brand_cd
    ,c.individual_id
    ,c.household_id as household_id
    ,c.residential_id as residential_id
    ,CASE c.child_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as child_IND
    ,CASE c.deceased_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as deceased_IND
    ,c.bill_to_ship_to_code as bill_to_ship_to_CD
    ,c.loyalty_no as loyalty_num
    ,c.plcc_status_code as plcc_status_CD
    ,c.plcc_card_type as plcc_card_type_cd
    ,to_date(from_unixtime(UNIX_TIMESTAMP(c.plcc_last_purchase_date,"MM/dd/yyyy"))) 
                                                                    as plcc_last_purchase_dt
    ,c.new_to_file_reactivated_code as new_to_file_reactivated_CD
    ,c.loyalty_store_no as loyalty_store_num
    ,c.trans_loyalty_store_12_mo as trans_loyalty_store_12_mo_cnt
    ,c.net_sales_lifetime_limited_too as net_sales_lifetime_limited_too_amt
    ,CASE c.do_not_contact_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as do_not_contact_IND
    ,CASE c.do_not_statement_insert_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END 
                                                                    as do_not_statement_insert_IND
    ,CASE c.do_not_sell_name_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END 
                                                                    as do_not_sell_name_IND
    ,CASE c.do_not_list_share_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END 
                                                                    as do_not_list_share_IND
    ,c.direct_mail_preference_code as direct_mail_preference_CD
    ,c.email_preference_code as email_preference_CD
    ,c.text_message_preference_code as text_message_preference_CD
    ,CASE c.mass_individual_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END 
                                                                    as mass_individual_IND
    ,c.corporate_individual_id as corporate_individual_id
    ,from_unixtime(UNIX_TIMESTAMP(c.create_date,"MM/dd/yyyy HH:mm:ss")) as create_tms
    ,c.create_source_code as create_source_CD
    ,c.create_job_id as create_job_id
    ,c.name_address_updt_source_code as name_address_updt_src_CD
    ,from_unixtime(UNIX_TIMESTAMP(c.last_update_time,"MM/dd/yyyy HH:mm:ss")) as last_update_tms
    ,c.last_update_source_code as last_update_source_CD
    ,c.last_update_job_id as last_update_job_id
    ,CASE c.coppa_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as coppa_IND
    ,CASE c.profanity_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END as profanity_IND
    --,max(CASE cb.ads_promotability_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END )
                                                                    --as ads_promotability_IND
    --,max(cb.ads_email_preference_code) as ads_email_preference_CD
    --,max(cb.ads_direct_mail_pref_code) as ads_direct_mail_pref_CD
    --,max(CASE cb.ads_do_not_statement_ins_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END )
                                                                    --as ads_do_not_statement_ins_IND
    --,max(CASE cb.ads_do_not_telemarket_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END )
                                                                    --as ads_do_not_telemarket_IND
    --,max(CASE cb.ads_do_not_sell_name_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END )
                                                                    --as ads_do_not_sell_name_IND
    --,max(CASE cb.ads_return_mail_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE NULL END )
                                                                    --as ads_return_mail_IND
    --,max(cb.ads_contact_pref_code) as ads_contact_pref_CD
    --,max(cb.ads_spam_pref_code) as ads_spam_pref_CD
    --,max(cb.open_to_buy) as open_to_buy
    ,c.batch_id as batch_id
 FROM ASCENA_STAGING_DEV.jus_hst_customers_drc as c
 --LEFT OUTER JOIN ascena_staging_dev.jus_hst_customer_bankcards_20190104 as cb
   --ON (c.individual_id = cb.individual_id)
  --and cb.bankcard_type_code = 'P' --where cb.bankcard_type_code = 'P' 
; 

--Primary Key check
--SELECT brand_customer_key
     --, COUNT(*)
  --FROM ascena_analytic_mart_dev.DIM_BRAND_CUSTOMER_20190104
 --GROUP BY brand_customer_key
--HAVING COUNT(brand_customer_key) >1
--;
