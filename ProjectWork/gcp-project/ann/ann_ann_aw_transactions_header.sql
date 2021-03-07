set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.ann_ann_aw_transactions_header (
 if_entry_no                decimal(13,0)         
 ,interface_control_flag     decimal(3,0)          
 ,record_type                string                
 ,store_no                   decimal(10,0)         
 ,register_no                decimal(5,0)          
 ,transaction_date           date                  
 ,entry_date_time            string                
 ,transaction_series         string                
 ,transaction_no             decimal(10,0)         
 ,cashier_no                 decimal(10,0)         
 ,transaction_category       decimal(3,0)          
 ,tender_total               decimal(13,0)         
 ,transaction_void_flag      decimal(5,0)          
 ,exception_flag             decimal(1,0)          
 ,deposit_declaration_flag   decimal(1,0)          
 ,closeout_flag              decimal(1,0)          
 ,media_count_flag           decimal(1,0)          
 ,tax_override_flag          decimal(1,0)          
 ,pos_tax_jurisdiction       string                
 ,employee_no                decimal(9,0)          
 ,transaction_remark         string                
 ,updated_by_user_name       string                
 ,company_no                 decimal(5,0)          
 ,till_no                    decimal(29,0)         
 ,batch_id                   bigint                           
 )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

set hivevar:dateval=${p1};

insert overwrite table ascena_analytic_mart.ann_ann_aw_transactions_header
select  if_entry_no               
 ,interface_control_flag    
 ,record_type               
 ,store_no                  
 ,register_no               
 ,transaction_date          
 ,entry_date_time           
 ,transaction_series        
 ,transaction_no            
 ,cashier_no                
 ,transaction_category      
 ,tender_total              
 ,transaction_void_flag     
 ,exception_flag            
 ,deposit_declaration_flag  
 ,closeout_flag             
 ,media_count_flag          
 ,tax_override_flag         
 ,pos_tax_jurisdiction      
 ,employee_no               
 ,transaction_remark        
 ,mask(updated_by_user_name) as updated_by_user_name   
 ,company_no                
 ,till_no                   
 ,batch_id                               
from ascena_staging.ann_ann_aw_transactions_header
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('${dateval}' as string),'yyyy-MM-dd')),'yyyy-MM-dd')
;