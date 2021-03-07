set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.brand_customer (
 brand_customer_key             string                
 ,individual_id                  bigint                
 ,brand_customer_id              string                
 ,nam_prefix                     string                
 ,first_nam                      string                
 ,middle_nam                     string                
 ,last_nam                       string                
 ,nam_generational_suffix        string                
 ,nam_professional_suffix        string                
 ,full_nam                       string                
 ,nam_source_type_cd             string                
 ,business_nam                   string                
 ,gender_cd                      string                
 ,birth_dt                       date                  
 ,crm_nam_prefix                 string                
 ,crm_first_nam                  string                
 ,crm_middle_nam                 string                
 ,crm_last_nam                   string                
 ,crm_nam_generational_suffix    string                
 ,crm_nam_professional_suffix    string                
 ,crm_gender_cd                  string                
 ,crm_business_nam               string                
 ,crm_deceased_ind               tinyint               
 ,crm_deceased_dt                date                  
 ,marital_status_cd              string                
 ,first_interaction_dt           date                  
 ,first_store_transaction_dt     date                  
 ,first_ecomm_transaction_dt     date                  
 ,ethinicity_cd                  string                
 ,religion_cd                    string                
 ,preferred_language_cd          string                
 ,origin_country_cd              string                
 ,profanity_ind                  tinyint               
 ,customer_store_key             string                
 ,possible_business_ind          tinyint               
 ,employee_ind                   tinyint               
 ,plcc_card_holder_ind           tinyint               
 ,brand_customer_creation_dt     date                  
 ,edl_create_tms                 timestamp             
 ,edl_create_job_nam             string                
 ,edl_last_update_tms            timestamp             
 ,edl_last_update_job_nam        string                
 ,brand_cd                       string                
 )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


insert overwrite table ascena_analytic_mart.brand_customer
select  brand_customer_key           
 ,individual_id                
 ,brand_customer_id            
 ,mask(nam_prefix) as nam_prefix              
 ,mask(first_nam)  as first_nam                  
 ,mask(middle_nam) as middle_nam              
 ,mask(last_nam)   as last_nam               
 ,mask(nam_generational_suffix) as nam_generational_suffix     
 ,mask(nam_professional_suffix) as nam_professional_suffix
 ,mask(full_nam) as full_nam                  
 ,nam_source_type_cd           
 ,mask(business_nam) as business_nam              
 ,mask(gender_cd) as gender_cd                  
 ,mask(birth_dt) as birth_dt                   
 ,mask(crm_nam_prefix) as  crm_nam_prefix              
 ,mask(crm_first_nam) as crm_first_nam                 
 ,mask(crm_middle_nam) as  crm_middle_nam              
 ,mask(crm_last_nam) as  crm_last_nam                
 ,mask(crm_nam_generational_suffix) as  crm_nam_generational_suffix
 ,mask(crm_nam_professional_suffix) as  crm_nam_professional_suffix  
 ,mask(crm_gender_cd) as   crm_gender_cd             
 ,mask(crm_business_nam) as  crm_business_nam          
 ,crm_deceased_ind             
 ,mask(crm_deceased_dt) as  crm_deceased_dt             
 ,marital_status_cd            
 ,first_interaction_dt         
 ,first_store_transaction_dt   
 ,first_ecomm_transaction_dt   
 ,ethinicity_cd                
 ,religion_cd                  
 ,preferred_language_cd        
 ,origin_country_cd            
 ,profanity_ind                
 ,customer_store_key           
 ,possible_business_ind        
 ,employee_ind                 
 ,plcc_card_holder_ind         
 ,brand_customer_creation_dt   
 ,edl_create_tms               
 ,edl_create_job_nam           
 ,edl_last_update_tms          
 ,edl_last_update_job_nam      
 ,brand_cd                     
from ascena_conform.brand_customer
where edl_last_update_tms > date_add(current_date,-3)
;