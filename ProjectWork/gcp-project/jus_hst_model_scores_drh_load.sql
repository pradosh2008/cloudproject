!record jus_hst_model_scores_drh.txt
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

use ascena_staging_dev;

--unzip all_twe_DR_MODEL_SCORES_CurrentMinus2_20190305.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus1_20190305.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus0_20190305.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus2_20181205.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus1_20181205.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus0_20181205.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus2_20180905.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus0_20180905.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus1_20180905.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus2_20180605.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus1_20180605.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus0_20180605.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus2_20180305.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus1_20180305.zip
--unzip all_twe_DR_MODEL_SCORES_CurrentMinus0_20180305.zip



--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus2_20190305.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus1_20190305.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus0_20190305.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus2_20181205.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus1_20181205.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus0_20181205.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus2_20180905.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus0_20180905.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus1_20180905.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus2_20180605.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus1_20180605.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus0_20180605.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus2_20180305.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus1_20180305.csv /ascena_dev/data/inbound/
--hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_MODEL_SCORES_CurrentMinus0_20180305.csv /ascena_dev/data/inbound/



CREATE TABLE ascena_staging_dev.jus_hst_model_scores_dr0(
 model_score_run_id       	 decimal(10,0)         
 ,original_household_id    	 decimal(11,0)         
 ,original_individual_id   	 decimal(11,0)         
 ,date_scored              	 string                
 ,model_type_code          	 string                
 ,division_code            	 string                
 ,model_score              	 decimal(12,6)         
 ,model_component_value_1  	 decimal(12,6)         
 ,model_component_value_2  	 decimal(14,12)        
 ,model_component_value_3  	 decimal(12,6)         
 ,model_component_value_4  	 decimal(6,0)          
 ,decile_0                 	 decimal(2,0)          
 ,decile_1                 	 decimal(2,0)          
 ,decile_2                 	 decimal(2,0)          
 ,decile_3                 	 decimal(2,0)          
 ,dependent_variable       	 decimal(1,0)
 ,batch_id bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
;

load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus2_20190305.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus1_20190305.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus0_20190305.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus2_20181205.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus1_20181205.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus0_20181205.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus2_20180905.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus0_20180905.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus1_20180905.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus2_20180605.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus1_20180605.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus0_20180605.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus2_20180305.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus1_20180305.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_MODEL_SCORES_CurrentMinus0_20180305.csv' into table ascena_staging_dev.jus_hst_model_scores_dr0;



create table if not exists ascena_staging_dev.jus_hst_model_scores_drh(
 model_score_run_id       	 decimal(10,0)         
 ,original_household_id    	 decimal(11,0)         
 ,original_individual_id   	 decimal(11,0)         
 ,date_scored              	 string                
 ,model_type_code          	 string                
 ,division_code            	 string                
 ,model_score              	 decimal(12,6)         
 ,model_component_value_1  	 decimal(12,6)         
 ,model_component_value_2  	 decimal(14,12)        
 ,model_component_value_3  	 decimal(12,6)         
 ,model_component_value_4  	 decimal(6,0)          
 ,decile_0                 	 decimal(2,0)          
 ,decile_1                 	 decimal(2,0)          
 ,decile_2                 	 decimal(2,0)          
 ,decile_3                 	 decimal(2,0)          
 ,dependent_variable       	 decimal(1,0)                                
)
partitioned by (batch_id bigint)
stored as orc
TBLPROPERTIES ("orc.compress"="SNAPPY");

insert overwrite table ascena_staging_dev.jus_hst_model_scores_drh partition(batch_id)
select
 model_score_run_id       
 ,original_household_id    
 ,original_individual_id   
 ,date_scored              
 ,model_type_code          
 ,division_code            
 ,model_score              
 ,model_component_value_1  
 ,model_component_value_2  
 ,model_component_value_3  
 ,model_component_value_4  
 ,decile_0                 
 ,decile_1                 
 ,decile_2                 
 ,decile_3                 
 ,dependent_variable       
 ,cast(concat(regexp_extract(input__file__name,'\\d{6}',0),'01') as bigint)  as batch_id
from ascena_staging_dev.jus_hst_model_scores_dr0
;
