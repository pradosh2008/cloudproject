set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.ann_ann_aw_transactions_merchdtl (
 if_entry_no                     decimal(12,0)         
 ,interface_control_flag          decimal(3,0)          
 ,record_type                     string                
 ,line_id                         decimal(5,0)          
 ,merchandise_category            decimal(4,0)          
 ,upc_lookup_division             decimal(2,0)          
 ,upc_no                          decimal(14,0)         
 ,sku_id                          decimal(14,0)         
 ,style_reference_id              decimal(13,0)         
 ,class_code                      decimal(10,0)         
 ,subclass_code                   decimal(5,0)          
 ,pos_identifier_type             decimal(3,0)          
 ,pos_identifier                  string                
 ,pos_dept_class                  decimal(10,0)         
 ,units                           decimal(15,0)         
 ,salesperson                     decimal(10,4)         
 ,salesperson2                    decimal(10,0)         
 ,ticket_price                    decimal(12,0)         
 ,sold_at_price                   decimal(12,0)         
 ,price_override                  string                
 ,upc_missing_in_pos_iplu_flag    string                
 ,scanned                         decimal(11,0)         
 ,upc_on_file                     decimal(1,0)          
 ,plu_price                       decimal(12,0)         
 ,origination_store_no            decimal(10,0)         
 ,source_store_no                 decimal(10,0)         
 ,fulfillment_store_no            decimal(10,0)         
 ,batch_id                        bigint                                                         
 )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

set hivevar:dateval=${p1};

insert overwrite table ascena_analytic_mart.ann_ann_aw_transactions_merchdtl
select  if_entry_no                   
 ,interface_control_flag        
 ,record_type                   
 ,line_id                       
 ,merchandise_category          
 ,upc_lookup_division           
 ,upc_no                        
 ,sku_id                        
 ,style_reference_id            
 ,class_code                    
 ,subclass_code                 
 ,pos_identifier_type           
 ,pos_identifier                
 ,pos_dept_class                
 ,units                         
 ,salesperson                   
 ,salesperson2                  
 ,ticket_price                  
 ,sold_at_price                 
 ,price_override                
 ,upc_missing_in_pos_iplu_flag  
 ,scanned                       
 ,upc_on_file                   
 ,plu_price                     
 ,origination_store_no          
 ,source_store_no               
 ,fulfillment_store_no          
 ,batch_id                                                                           
from ascena_staging.ann_ann_aw_transactions_merchdtl
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('${dateval}' as string),'yyyy-MM-dd')),'yyyy-MM-dd')
;