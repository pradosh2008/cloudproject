
--we need a separate ddl script to create ascena_analytic_mart.brand_customer_phone

insert overwrite table ascena_analytic_mart.brand_customer_phone
select brand_customer_key
    ,phone_key
    ,mask(phone_num)            as phone_num
    ,mask(phone_extn_num)       as phone_extn_num
    ,phone_type_cd
    ,phone_verification_cd
    ,phone_verification_dt
    ,phone_status_cd
    ,phone_category_cd
    ,phone_source_type_cd
    ,primary_ind
    ,unlisted_ind
    ,text_message_opt_out_ind
    ,last_text_message_opt_out_tm
    ,do_not_call_ind
    ,last_do_not_call_tms
    ,pa_opt_out_ind
    ,crm_do_not_call_ind
    ,crm_last_do_not_call_tm
    ,ph_create_tms
    ,edl_create_tms
    ,edl_create_job_nam
    ,edl_last_update_tms
    ,edl_last_update_job_nam
    ,brand_cd
from ascena_conform.brand_customer_phone
where edl_last_update_tms > date_add(current_date,-3)
;
