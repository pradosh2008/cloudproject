#!/usr/bin/bash

. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh

#creating the landing table
#bq load --autodetect --replace --source_format=CSV --quote="" edl_landing.ann_acc_df_prod_hier 'gs://p-asna-datasink-003/pre/ANN_ACC_DF_PROD_HIER_*.txt'

bq load --replace=true --source_format=CSV --field_delimiter="|" --quote="" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_stage/ann_acc_df_prod_hier.json edl_landing.ann_acc_df_prod_hier \
        'gs://p-asna-datasink-003/pre/ANN_ACC_DF_PROD_HIER_*.txt'

rc_check $? "Load edl_landing"

bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_stage.pre_acc_df_prod_hier_new --use_legacy_sql=false <<!
select
         cast(subclass_id                             as string)        as subclass_id         
        ,cast(subclass_name                         as string)      as subclass_name     
        ,cast(class_id                                 as string)      as class_id             
        ,cast(class_name                             as string)      as class_name         
        ,cast(dept_id                                as string)      as dept_id            
        ,cast(dept_name                                as string)        as dept_name            
        ,cast(group_id                                as string)        as group_id            
        ,cast(group_name                            as string)        as group_name        
        ,cast(div_id                                as string)        as div_id            
        ,cast(div_name                                as string)        as div_name            
        ,cast(brand_id                                as string)        as brand_id            
        ,cast(brand_name                            as string)        as brand_name        
        ,cast(style_id                                as string)        as style_id            
        ,cast(style_desc                            as string)        as style_desc        
        ,cast(color_id                                as string)        as color_id            
        ,cast(color_desc                            as string)        as color_desc        
        ,cast(store_set_1                            as string)        as store_set_1        
        ,cast(store_set_2                            as string)        as store_set_2        
        ,cast(ecomm_exclusive                        as string)      as ecomm_exclusive    
        ,cast(test_ind                                 as string)      as test_ind             
        ,cast(fashion_pyramid                        as string)      as fashion_pyramid	
      --,cast(FORMAT_Date('%Y%m%d',current_date)	as int64)      as batch_id      
        ,20191208      as batch_id
from edl_landing.ann_acc_df_prod_hier
!

rc_check $? "creating temp table"

#bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --append_table --destination_table edl_stage.pre_acc_df_prod_hier_new --use_legacy_sql=false <<!
#select c.*
#from edl_stage.pre_sap_prod_hier c
#left join edl_stage.ann_prod_hier_new w
#     on  w.class_id = c.class_id
#     and w.style_id = c.style_id
#     and w.color_id = c.color_id
#     and w.batch_id = c.batch_id
#    where w.class_id is null
#!

bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --append_table --destination_table edl_stage.pre_acc_df_prod_hier_new --use_legacy_sql=false <<!
select c.*
from edl_stage.pre_acc_df_prod_hier c
!

rc_check $? "append legacy records into temp table"


#cleansing and archival
bq cp --force edl_stage.pre_acc_df_prod_hier edl_archive.pre_acc_df_prod_hier
rc_check $? "archive copy"
bq cp --force edl_stage.pre_acc_df_prod_hier_new edl_stage.pre_acc_df_prod_hier
rc_check $? "replace the temp table as the stage table"
bq rm --force edl_stage.pre_acc_df_prod_hier_new
rc_check $? "drop the temp table"

#move the files to pre/archive folder
gsutil mv gs://p-asna-datasink-003/pre/ANN_ACC_DF_PROD_HIER_*.txt  gs://p-asna-datasink-003/pre/archive
rc_check $? "move the processed file to archive folder"

