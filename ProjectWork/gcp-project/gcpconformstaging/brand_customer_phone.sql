set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.brand_customer_phone (
 brand_customer_key              string                
 ,phone_key                       string                
 ,phone_num                       string                
 ,phone_extn_num                  string                
 ,phone_type_cd                   string                
 ,phone_verification_cd           string                
 ,phone_verification_dt           date                  
 ,phone_status_cd                 string                
 ,phone_category_cd               string                
 ,phone_source_type_cd            string                
 ,primary_ind                     tinyint               
 ,unlisted_ind                    tinyint               
 ,text_message_opt_out_ind        tinyint               
 ,last_text_message_opt_out_tm    timestamp             
 ,do_not_call_ind                 tinyint               
 ,last_do_not_call_tms            timestamp             
 ,pa_opt_out_ind                  tinyint               
 ,crm_do_not_call_ind             tinyint               
 ,crm_last_do_not_call_tm         timestamp             
 ,ph_create_tms                   timestamp             
 ,edl_create_tms                  timestamp             
 ,edl_create_job_nam              string                
 ,edl_last_update_tms             timestamp             
 ,edl_last_update_job_nam         string                
 ,brand_cd                        string                

)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


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