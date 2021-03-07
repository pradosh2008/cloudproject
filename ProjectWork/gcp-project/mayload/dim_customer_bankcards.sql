--/*********************************************************************
--* 
--* Script Name: DIM_CUSTOMER_BANKCARDS table creation and data population
--*
--**********************************************************************/
--/*
--md5(concat('JUS|', coalesce(b.bankcard_acct_token, ''), '|', b.individual_id, '|', coalesce(b.account_number, '')))

set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
set mapred.output.compression.type=BLOCK;
!set force true;

drop TABLE ascena_analytic_mart_dev.dim_customer_bankcards;

CREATE TABLE ascena_analytic_mart_dev.dim_customer_bankcards (
bankcard_key CHAR(32)
, brand_customer_key CHAR(32)
, bankcard_type_cd string
, invalid_ind INT
, create_dt date
, create_job_id decimal(10,0)
, last_update_tms timestamp
, last_update_source_cd string
, last_update_job_id decimal(10,0)
, account_open_dt date
, account_closed_dt string
, ads_promotability_int INT
, ads_email_preference_cd string
, ads_direct_mail_pref_cd string
, ads_do_not_statement_ins_ind INT
, ads_do_not_telemarket_ind INT
, ads_do_not_sell_name_ind INT
, ads_return_mail_ind INT
, ads_contact_pref_cd string
, ads_spam_pref_cd string
, open_to_buy_num decimal(17,0)
, crlim_num string
, channel_logo_num string
, original_store_num decimal(6,0)
, record_type_ind INT
, promotable_ind INT
, conversant_extract_status_cd string
, batch_id BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy");

--Temporary table creation 
create temporary table ASCENA_STAGING_DEV.jus_hst_customer_bankcards_tmp as
    select bankcard_acct_token
       ,individual_id
       ,account_number
        ,batch_id
    from ASCENA_STAGING_DEV.jus_hst_customer_bankcards_drc
;

INSERT OVERWRITE TABLE ascena_analytic_mart_dev.dim_customer_bankcards
SELECT md5(concat('JUS|', coalesce(upper(trim(b.bankcard_acct_token)), ''), '|', coalesce(b.individual_id, ''), '|', coalesce(b.account_number, ''))) as bankcard_key
, md5(concat('JUS|', coalesce(b.individual_id, ''))) as brand_customer_key
, b.bankcard_type_code as bankcard_type_cd
, CASE b.invalid_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as invalid_ind
, from_unixtime(UNIX_TIMESTAMP(b.create_date,"MM/dd/yyyy HH:mm:ss")) as create_dt
, b.create_job_id as create_job_id
, from_unixtime(UNIX_TIMESTAMP(b.last_update_time,"MM/dd/yyyy HH:mm:ss")) as last_update_tms
, b.last_update_source_code as last_update_source_cd
, b.last_update_job_id as last_update_job_id
, from_unixtime(UNIX_TIMESTAMP(b.acct_open_dt,"MM/dd/yyyy HH:mm:ss")) as account_open_dt
, from_unixtime(UNIX_TIMESTAMP(b.acct_closed_dt,"MM/dd/yyyy HH:mm:ss")) as account_closed_dt
, CASE b.ads_promotability_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as ads_promotability_IND
, b.ads_email_preference_code as ads_email_preference_CD
, b.ads_direct_mail_pref_code as ads_direct_mail_pref_CD
, CASE b.ads_do_not_statement_ins_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as ads_do_not_statement_ins_IND
, CASE b.ads_do_not_telemarket_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as ads_do_not_telemarket_IND
, CASE b.ads_do_not_sell_name_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as ads_do_not_sell_name_IND
, CASE b.ads_return_mail_flag WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as ads_return_mail_IND
, b.ads_contact_pref_code as ads_contact_pref_CD
, b.ads_spam_pref_code as ads_spam_pref_CD
, b.open_to_buy as open_to_buy_num
, b.crlim as crlim_num
, b.channel_logo as channel_logo_num
, b.orig_store_nbr as original_store_num
, CASE b.record_type_ind WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as record_type_ind
, CASE b.promotable_ind WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as promotable_ind
, b.conversant_extract_status as conversant_extract_status_cd
, b.batch_id as batch_id
FROM ASCENA_STAGING_DEV.jus_hst_customer_bankcards_dr as b
 join ASCENA_STAGING_DEV.jus_hst_customer_bankcards_tmp as f
 on coalesce(b.individual_id, '')=coalesce(f.individual_id, '')
 and coalesce(upper(trim(b.bankcard_acct_token)), '')=coalesce(upper(trim(f.bankcard_acct_token)), '')
 and coalesce(b.account_number, '')=coalesce(f.account_number, '')
 and b.batch_id=f.batch_id
--WHERE b.bankcard_type_code IN ('B', 'P')
;