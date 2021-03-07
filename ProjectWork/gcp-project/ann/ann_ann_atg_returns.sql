set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;

/* not using
CREATE TABLE IF NOT EXISTS ascena_analytic_mart.ann_ann_atg_returns (
 returnid                   string                
 ,orderid                    string                
 ,replacementorderid         string                
 ,createddate                string                
 ,actualtaxrefund            decimal(10,2)         
 ,actualshiprefund           decimal(10,2)         
 ,otherrefund                decimal(10,2)         
 ,processimmediately         decimal(6,0)          
 ,processed                  decimal(6,0)          
 ,originofreturns            decimal(6,0)          
 ,returnfee                  decimal(10,2)         
 ,commerceitemid             string                
 ,shippinggroupid            string                
 ,quantitytoreturn           decimal(6,0)          
 ,quantitytoreplace          decimal(6,0)          
 ,isreturnshippingrequired   decimal(6,0)          
 ,quantityreceived           decimal(6,0)          
 ,refundamount               decimal(10,2)         
 ,actualtaxrefunditem        decimal(10,2)         
 ,actualshiprefunditem       decimal(10,2)         
 ,bonusrefund                decimal(10,2)         
 ,sumoftotalrefundamount     decimal(10,2)         
 ,method                     decimal(6,0)          
 ,shipping_label_fee         decimal(10,2)         
 ,batch_id                   bigint                
 )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;
*/

CREATE TABLE IF NOT EXISTS ascena_analytic_mart.ann_ann_atg_returns (
 returnid                       string
 ,orderid                       string
 ,replacementorderid            string
 ,createddate                   string
 ,actualtaxrefund               string
 ,actualshiprefund              string
 ,otherrefund                   string
 ,processimmediately            string
 ,processed                     string
 ,originofreturns               string
 ,returnfee                     string
 ,commerceitemid                string
 ,shippinggroupid               string
 ,quantitytoreturn              string
 ,quantitytoreplace             string
 ,isreturnshippingrequired      string
 ,quantityreceived              string
 ,refundamount                  string
 ,actualtaxrefunditem           string
 ,actualshiprefunditem          string
 ,bonusrefund                   string
 ,sumoftotalrefundamount        string
 ,method                        string
 ,shipping_label_fee            string
 ,batch_id                      bigint
 )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;



set hivevar:days=${p1};


insert overwrite table ascena_analytic_mart.ann_ann_atg_returns
select   returnid                  
 ,orderid                   
 ,replacementorderid        
 ,createddate               
 ,actualtaxrefund           
 ,actualshiprefund          
 ,otherrefund               
 ,processimmediately        
 ,processed                 
 ,originofreturns           
 ,returnfee                 
 ,commerceitemid            
 ,shippinggroupid           
 ,quantitytoreturn          
 ,quantitytoreplace         
 ,isreturnshippingrequired  
 ,quantityreceived          
 ,refundamount              
 ,actualtaxrefunditem       
 ,actualshiprefunditem      
 ,bonusrefund               
 ,sumoftotalrefundamount    
 ,method                    
 ,shipping_label_fee        
 ,batch_id                  
from ascena_staging.ann_ann_atg_returns
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) > date_sub(current_date,${days})
;


