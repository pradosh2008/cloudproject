!record jus_hst_cdi_key_xref_drh_new.txt
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

use ascena_staging_dev;

-- unzip all_twe_DR_Cdi_Key_Xref_20190304.zip
-- unzip all_twe_DR_Cdi_Key_Xref_20181204.zip
-- unzip all_twe_DR_Cdi_Key_Xref_20180904.zip
-- unzip all_twe_DR_Cdi_Key_Xref_20180604.zip
-- unzip all_twe_DR_Cdi_Key_Xref_20180304.zip

-- hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_Cdi_Key_Xref_20190304.csv /ascena_dev/data/inbound/
-- hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_Cdi_Key_Xref_20181204.csv /ascena_dev/data/inbound/
-- hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_Cdi_Key_Xref_20180904.csv /ascena_dev/data/inbound/
-- hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_Cdi_Key_Xref_20180604.csv /ascena_dev/data/inbound/
-- hdfs dfs -put /ascena_dev/data/inbound/cem/dr/all_twe_DR_Cdi_Key_Xref_20180304.csv /ascena_dev/data/inbound/


CREATE TABLE ASCENA_STAGING_DEV.jus_hst_cdi_key_xref_dr0(
 original_key_source      	 string                
 ,original_individual_id   	 decimal(11,0)         
 ,original_household_id    	 decimal(11,0)         
 ,original_residential_id  	 decimal(11,0)         
 ,current_individual_id    	 decimal(11,0)         
 ,current_household_id     	 decimal(11,0)         
 ,current_residential_id   	 decimal(11,0)         
 ,assigned_key_source      	 string                
 ,assigned_individual_id   	 decimal(11,0)         
 ,create_date              	 string                
 ,create_job_id            	 decimal(10,0)         
 ,last_update_date         	 string                
 ,last_update_job_id       	 decimal(10,0)         
 ,batch_id                 	 bigint                
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
;

load data inpath '/ascena_dev/data/inbound/all_twe_DR_Cdi_Key_Xref_20190304.csv' into table ascena_staging_dev.jus_hst_cdi_key_xref_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_Cdi_Key_Xref_20181204.csv' into table ascena_staging_dev.jus_hst_cdi_key_xref_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_Cdi_Key_Xref_20180904.csv' into table ascena_staging_dev.jus_hst_cdi_key_xref_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_Cdi_Key_Xref_20180604.csv' into table ascena_staging_dev.jus_hst_cdi_key_xref_dr0;
load data inpath '/ascena_dev/data/inbound/all_twe_DR_Cdi_Key_Xref_20180304.csv' into table ascena_staging_dev.jus_hst_cdi_key_xref_dr0;


create table if not exists ASCENA_STAGING_DEV.jus_hst_cdi_key_xref_drh_new(
 original_key_source      	 string                
 ,original_individual_id   	 decimal(11,0)         
 ,original_household_id    	 decimal(11,0)         
 ,original_residential_id  	 decimal(11,0)         
 ,current_individual_id    	 decimal(11,0)         
 ,current_household_id     	 decimal(11,0)         
 ,current_residential_id   	 decimal(11,0)         
 ,assigned_key_source      	 string                
 ,assigned_individual_id   	 decimal(11,0)         
 ,create_date              	 string                
 ,create_job_id            	 decimal(10,0)         
 ,last_update_date         	 string                
 ,last_update_job_id       	 decimal(10,0)                               
)
partitioned by (batch_id bigint)
stored as orc
TBLPROPERTIES ("orc.compress"="SNAPPY");

insert overwrite table ASCENA_STAGING_DEV.jus_hst_cdi_key_xref_drh_new partition(batch_id)
select
 original_key_source      
 ,original_individual_id   
 ,original_household_id    
 ,original_residential_id  
 ,current_individual_id    
 ,current_household_id     
 ,current_residential_id   
 ,assigned_key_source      
 ,assigned_individual_id   
 ,create_date              
 ,create_job_id            
 ,last_update_date         
 ,last_update_job_id       
 ,cast(concat(regexp_extract(input__file__name,'\\d{6}',0),'01') as bigint)  as batch_id
from ascena_staging_dev.jus_hst_cdi_key_xref_dr0
;
