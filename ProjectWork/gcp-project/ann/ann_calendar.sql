set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.ann_calendar (
 day_dt                     timestamp             
 ,day_of_week                decimal(1,0)          
 ,fiscal_day_of_week         decimal(1,0)          
 ,day_of_month               decimal(2,0)          
 ,day_of_year                decimal(3,0)          
 ,julian_day                 decimal(8,0)          
 ,day_nm                     string                
 ,day_abbr                   string                
 ,prev_day_dt                timestamp             
 ,lw_day_dt                  timestamp             
 ,lm_day_dt                  timestamp             
 ,lq_day_dt                  timestamp             
 ,ly_day_dt                  timestamp             
 ,week_id                    decimal(6,0)          
 ,week_of_year               decimal(2,0)          
 ,week_of_month              decimal(1,0)          
 ,fiscal_week_id             decimal(6,0)          
 ,fiscal_week_of_year        decimal(2,0)          
 ,month_id                   decimal(6,0)          
 ,month_of_year              decimal(2,0)          
 ,month_nm                   string                
 ,month_abbr                 string                
 ,fiscal_month_id            decimal(6,0)          
 ,fiscal_month_of_year       decimal(2,0)          
 ,fiscal_month_nm            string                
 ,month_duration             decimal(2,0)          
 ,prev_month_id              decimal(6,0)          
 ,lq_month_id                decimal(6,0)          
 ,ly_month_id                decimal(6,0)          
 ,quarter_id                 decimal(5,0)          
 ,quarter_nbr                decimal(1,0)          
 ,quarter_nm                 string                
 ,fiscal_quarter_id          decimal(5,0)          
 ,fiscal_quarter_nbr         decimal(1,0)          
 ,quarter_duration           decimal(2,0)          
 ,prev_quarter_id            decimal(5,0)          
 ,ly_quarter_id              decimal(5,0)          
 ,year_id                    decimal(4,0)          
 ,fiscal_year_id             decimal(4,0)          
 ,year_duration              decimal(3,0)          
 ,prev_year_id               decimal(4,0)          
 ,batch_id                   bigint )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;



insert overwrite table ascena_analytic_mart.ann_calendar
select  day_dt                   
 ,day_of_week              
 ,fiscal_day_of_week       
 ,day_of_month             
 ,day_of_year              
 ,julian_day               
 ,day_nm                   
 ,day_abbr                 
 ,prev_day_dt              
 ,lw_day_dt                
 ,lm_day_dt                
 ,lq_day_dt                
 ,ly_day_dt                
 ,week_id                  
 ,week_of_year             
 ,week_of_month            
 ,fiscal_week_id           
 ,fiscal_week_of_year      
 ,month_id                 
 ,month_of_year            
 ,month_nm                 
 ,month_abbr               
 ,fiscal_month_id          
 ,fiscal_month_of_year     
 ,fiscal_month_nm          
 ,month_duration           
 ,prev_month_id            
 ,lq_month_id              
 ,ly_month_id              
 ,quarter_id               
 ,quarter_nbr              
 ,quarter_nm               
 ,fiscal_quarter_id        
 ,fiscal_quarter_nbr       
 ,quarter_duration         
 ,prev_quarter_id          
 ,ly_quarter_id            
 ,year_id                  
 ,fiscal_year_id           
 ,year_duration            
 ,prev_year_id             
 ,batch_id                                                                     
from ascena_staging.ann_hst_lu_day_vw;