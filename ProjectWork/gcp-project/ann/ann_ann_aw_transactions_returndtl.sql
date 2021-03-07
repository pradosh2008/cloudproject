set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.ann_ann_aw_transactions_returndtl (
 if_entry_no                     decimal(12,0)         
 ,interface_control_flag          decimal(3,0)          
 ,record_type                     string                
 ,line_id                         decimal(5,0)          
 ,return_reason_message           string                
 ,return_reason_code              decimal(5,0)          
 ,merchandise_disposition_code    decimal(5,0)          
 ,via_warehouse                   string                
 ,original_salesperson            decimal(10,0)         
 ,original_salesperson2           decimal(10,0)         
 ,return_from_store               decimal(10,0)         
 ,return_from_register            decimal(5,0)          
 ,return_from_date                date                  
 ,return_from_transaction_no      decimal(10,0)         
 ,without_receipt_flag            string                
 ,batch_id                        bigint                                          
 )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

set hivevar:dateval=${p1};

insert overwrite table ascena_analytic_mart.ann_ann_aw_transactions_returndtl
select if_entry_no                   
 ,interface_control_flag        
 ,record_type                   
 ,line_id                       
 ,return_reason_message         
 ,return_reason_code            
 ,merchandise_disposition_code  
 ,via_warehouse                 
 ,original_salesperson          
 ,original_salesperson2         
 ,return_from_store             
 ,return_from_register          
 ,return_from_date              
 ,return_from_transaction_no    
 ,without_receipt_flag          
 ,batch_id                                                     
from ascena_staging.ann_ann_aw_transactions_returndtl
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('${dateval}' as string),'yyyy-MM-dd')),'yyyy-MM-dd')
;