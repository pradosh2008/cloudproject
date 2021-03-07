set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.brand_customer_account (
 brand_customer_key         string                
 ,account_id                 string                
 ,application_num            bigint                
 ,card_type_cd               string                
 ,card_sub_type_cd           string                
 ,card_type_des              string                
 ,card_cobranded_ind         tinyint               
 ,card_bin_num               int                   
 ,card_last4_num             int                   
 ,card_account_type_cd       string                
 ,card_status_cd             string                
 ,card_credit_limit_amt      float                 
 ,card_applied_dt            date                  
 ,card_issued_dt             date                  
 ,card_opened_dt             date                  
 ,card_closed_dt             date                  
 ,housecard_ind              tinyint               
 ,card_open_to_buy_cd        string                
 ,card_closed_ind            tinyint               
 ,card_active_ind            tinyint               
 ,edl_create_tms             timestamp             
 ,edl_create_job_nam         string                
 ,edl_last_update_tms        timestamp             
 ,edl_last_update_job_nam    string                
 ,brand_cd                   string                
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


insert overwrite table ascena_analytic_mart.brand_customer_account
select  brand_customer_key       
 ,mask(account_id) as account_id              
 ,mask(application_num) as application_num          
 ,card_type_cd             
 ,card_sub_type_cd         
 ,card_type_des            
 ,card_cobranded_ind       
 ,mask(card_bin_num) as card_bin_num            
 ,mask(card_last4_num)  as card_last4_num        
 ,card_account_type_cd     
 ,card_status_cd           
 ,mask(card_credit_limit_amt) as  card_credit_limit_amt
 ,mask(card_applied_dt) as card_applied_dt       
 ,card_issued_dt           
 ,card_opened_dt           
 ,card_closed_dt           
 ,housecard_ind            
 ,card_open_to_buy_cd      
 ,card_closed_ind          
 ,card_active_ind          
 ,edl_create_tms           
 ,edl_create_job_nam       
 ,edl_last_update_tms      
 ,edl_last_update_job_nam  
 ,brand_cd                 
from ascena_conform.brand_customer_account
where edl_last_update_tms > date_add(current_date,-3)
;