-- select distinct * from edl_stage.pre_store_inventory --20168402 
   select count(*) from edl_stage.pre_store_inventory --20168402

--SELECT count(*) FROM `p-asna-analytics-002.demand_forecast.inventory_store_sku` LIMIT 1000 --1825059766

--SELECT min(week_id),max(week_id) FROM `p-asna-analytics-002.demand_forecast.inventory_store_sku`  
--201601   201914  

--SELECT min(week_id),max(week_id) FROM edl_stage.pre_store_inventory  
--201927  201929 

SELECT count(*) FROM  demand_forecast.inventory_store_sku; --1825059766


create or replace table edl_stage.pre_ann_calendar
as select
cast(day_dt as date) as day_dt
,day_of_week
,fiscal_day_of_week
,day_of_month
,day_of_year
,julian_day
,day_nm
,day_abbr
,cast(prev_day_dt as date) as prev_day_dt
,cast(lw_day_dt as date) as lw_day_dt
,cast(lm_day_dt as date) as lm_day_dt
,cast(lq_day_dt as date) as lq_day_dt
,cast(ly_day_dt as date) as ly_day_dt
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
from edl_archive.pre_ann_calendar;


--test with store dimension 

select * from demand_forecast.inventory_store_sku a left outer join (select cast(store_no as int64) as store_no from  edl_stage.pre_sap_store 
where store_no NOT LIKE 'A%' AND store_no NOT LIKE 'R%') as b
on a.store_id=b.store_no where b.store_no is null




create or replace table analytic_mart_dev.pre_inventory_store
as select
 cast(chain_id as int64) as chain_id
,FARM_FINGERPRINT(cast(cast(store_id as int64) as string)) as store_key
,cast(week_id  as int64) as week_id
,FARM_FINGERPRINT(cast(cast(style_id as int64) as string)) as style_key 
,cast(color_id as int64) as color_id
,cast(size_id  as int64) as size_id
,FARM_FINGERPRINT(cast(cast(sku_id as int64) as string)) as item_key   
,bop_store_inv_cnt   
,cast(bop_store_inv_amt as numeric)    as bop_store_inv_amt
,cast(bop_store_inv_actual_amt as numeric)    as bop_store_inv_actual_amt
,cast(bop_store_inv_value_amt as numeric)    as bop_store_inv_value_amt
,bop_store_it_cnt   
,cast(bop_store_it_amt as numeric)    as bop_store_it_amt
,cast(bop_store_it_actual_amt as numeric)    as bop_store_it_actual_amt
,cast(bop_store_it_value_amt as numeric)    as bop_store_it_value_amt
,store_receipt_cnt   
,cast(store_receipt_amt as numeric)    as store_receipt_amt
,cast(store_receipt_actual_amt as numeric)    as store_receipt_actual_amt
,cast(store_receipt_value_amt as numeric)    as store_receipt_value_amt
,bop_mkd_store_it_cnt   
,cast(bop_mkd_store_it_amt as numeric)    as bop_mkd_store_it_amt
,cast(bop_mkd_store_it_actual_amt as numeric)    as bop_mkd_store_it_actual_amt
,cast(bop_mkd_store_it_value_amt as numeric)    as bop_mkd_store_it_value_amt
,dmos_cnt   
,cast(dmos_amt as numeric)    as dmos_amt
,cast(dmos_actual_amt as numeric)    as dmos_actual_amt
,cast(dmos_value_amt as numeric)    as dmos_value_amt
,store_trans_out_cnt   
,cast(store_trans_out_amt as numeric)    as store_trans_out_amt
,cast(store_trans_out_actual_amt as numeric)    as store_trans_out_actual_amt
,cast(store_trans_out_value_amt as numeric)    as store_trans_out_value_amt
,store_trans_in_cnt   
,cast(store_trans_in_amt as numeric)    as store_trans_in_amt
,cast(store_trans_in_actual_amt as numeric)    as store_trans_in_actual_amt
,cast(store_trans_in_value_amt as numeric)    as store_trans_in_value_amt
,bop_alloc_notship_inv_cnt   
,cast(bop_alloc_notship_inv_amt as numeric)    as bop_alloc_notship_inv_amt
,cast(bop_alloc_notship_actual_amt as numeric)    as bop_alloc_notship_actual_amt
,cast(bop_alloc_notship_value_amt as numeric)    as bop_alloc_notship_value_amt
,mkd_issue_cnt   
,cast(mkd_issue_amt as numeric)    as mkd_issue_amt
,cast(mkd_issue_cost_amt as numeric)    as mkd_issue_cost_amt
,cast(bop_store_mkd_it_cnt as numeric)    as bop_store_mkd_it_cnt
,cast(bop_store_mkd_it_amt as numeric)    as bop_store_mkd_it_amt
,cast(bop_store_mkd_it_actual_amt as numeric)    as bop_store_mkd_it_actual_amt
from demand_forecast.inventory_store_sku;




-----------------------------------------------------mart inventory script -----------------------------------------------------





Action item 
-- color_id and size_id remove  
-- instead of week_id place fiscal_week_dt
-- or else have a week dimension
-- check is the c is cost r as retail u as unit 


#staging 
SELECT 
   --date_trunc(cast(day_dt as date),year)
   year(from_unixtime(UNIX_TIMESTAMP(day_dt,"MM/dd/yyyy")))
  ,count(*)
FROM ascena_staging.ann_hst_lu_day_vw 
group by year(from_unixtime(UNIX_TIMESTAMP(day_dt,"MM/dd/yyyy")))
order by 1
LIMIT 1000
-- check it on hadoop 

--------------------------------------------------- beginning of program ----------------------------------------------------------

-------------------------------- one time task----------- --------
#create or replace table edl_stage.pre_inventory_store
#as select
# cast(chain_id as int64) as chain_id
#,cast(store_id as int64) as store_id
#,cast(week_id  as int64) as week_id
#,cast(style_id as int64) as style_id 
#,cast(color_id as int64) as color_id
#,cast(size_id  as int64) as size_id
#,cast(sku_id as int64) as sku_id   
#,bop_store_inv_cnt   
#,cast(bop_store_inv_amt as numeric)    as bop_store_inv_amt
#,cast(bop_store_inv_actual_amt as numeric)    as bop_store_inv_actual_amt
#,cast(bop_store_inv_value_amt as numeric)    as bop_store_inv_value_amt
#,bop_store_it_cnt   
#,cast(bop_store_it_amt as numeric)    as bop_store_it_amt
#,cast(bop_store_it_actual_amt as numeric)    as bop_store_it_actual_amt
#,cast(bop_store_it_value_amt as numeric)    as bop_store_it_value_amt
#,store_receipt_cnt   
#,cast(store_receipt_amt as numeric)    as store_receipt_amt
#,cast(store_receipt_actual_amt as numeric)    as store_receipt_actual_amt
#,cast(store_receipt_value_amt as numeric)    as store_receipt_value_amt
#,bop_mkd_store_it_cnt   
#,cast(bop_mkd_store_it_amt as numeric)    as bop_mkd_store_it_amt
#,cast(bop_mkd_store_it_actual_amt as numeric)    as bop_mkd_store_it_actual_amt
#,cast(bop_mkd_store_it_value_amt as numeric)    as bop_mkd_store_it_value_amt
#,dmos_cnt   
#,cast(dmos_amt as numeric)    as dmos_amt
#,cast(dmos_actual_amt as numeric)    as dmos_actual_amt
#,cast(dmos_value_amt as numeric)    as dmos_value_amt
#,store_trans_out_cnt   
#,cast(store_trans_out_amt as numeric)    as store_trans_out_amt
#,cast(store_trans_out_actual_amt as numeric)    as store_trans_out_actual_amt
#,cast(store_trans_out_value_amt as numeric)    as store_trans_out_value_amt
#,store_trans_in_cnt   
#,cast(store_trans_in_amt as numeric)    as store_trans_in_amt
#,cast(store_trans_in_actual_amt as numeric)    as store_trans_in_actual_amt
#,cast(store_trans_in_value_amt as numeric)    as store_trans_in_value_amt
#,bop_alloc_notship_inv_cnt   
#,cast(bop_alloc_notship_inv_amt as numeric)    as bop_alloc_notship_inv_amt
#,cast(bop_alloc_notship_actual_amt as numeric)    as bop_alloc_notship_actual_amt
#,cast(bop_alloc_notship_value_amt as numeric)    as bop_alloc_notship_value_amt
#,mkd_issue_cnt   
#,cast(mkd_issue_amt as numeric)    as mkd_issue_amt
#,cast(mkd_issue_cost_amt as numeric)    as mkd_issue_cost_amt
#,cast(bop_store_mkd_it_cnt as int64)    as bop_store_mkd_it_cnt
#,cast(bop_store_mkd_it_amt as numeric)    as bop_store_mkd_it_amt
#,cast(bop_store_mkd_it_actual_amt as numeric)    as bop_store_mkd_it_actual_amt
#from demand_forecast.inventory_store_sku;

--------------------------------------------------------------------------------------------------------------------------------
Staging load 
--------------

#bq load --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
#        --schema=/home/gcpinteg/script/storeinventoryschema.json edl_landing.ann_sku_store_inventory \
#        'gs://p-asna-datasink-003/pre/sap/ANN_ACC_EDL_STORE_INVENTORY_*.txt'


bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_stage.edl_stage.ann_sku_store_inventory_new --use_legacy_sql=false <<!
select
cast(chain_id as int64) as chain_id
,cast(store_id as int64) as store_id
,cast(week_id as int64) as week_id
,cast(style_id as int64) as style_id
,cast(color_id as int64) as color_id
,cast(size_id as int64) as size_id
,cast(sku_id as int64) as sku_id
,cast(bop_store_inv_u as int64) as bop_store_inv_unit_cnt
,cast(bop_store_inv_r as numeric) as bop_store_inv_retail_amt
,cast(bop_store_inv_c_actual as numeric) as bop_store_inv_actual_cost_amt
,cast(bop_store_inv_c_valued as numeric) as bop_store_inv_valued_cost_amt
,cast(bop_store_it_u as int64) as bop_store_it_unit_cnt
,cast(bop_store_it_r as numeric) as bop_store_it_retail_amt
,cast(bop_store_it_c_actual as numeric) as bop_store_it_actual_cost_amt
,cast(bop_store_it_c_valued as numeric) as bop_store_it_valued_cost_amt
,cast(store_receipt_u as int64) as store_receipt_unit_cnt
,cast(store_receipt_r as numeric) as store_receipt_retail_amt
,cast(store_receipt_c_actual as numeric) as store_receipt_actual_cost_amt
,cast(store_receipt_c_valued as numeric) as store_receipt_valued_cost_amt
,cast(bop_mkd_store_it_u as int64) as bop_mkd_store_it_unit_cnt
,cast(bop_mkd_store_it_r as numeric) as bop_mkd_store_it_retail_amt
,cast(bop_mkd_store_it_c_actual as numeric) as bop_mkd_store_it_actual_cost_amt
,cast(bop_mkd_store_it_c_valued as numeric) as bop_mkd_store_it_valued_cost_amt
,cast(dmos_u as int64) as dmos_unit_cnt
,cast(dmos_r as numeric) as dmos_retail_amt
,cast(dmos_c_actual as numeric) as dmos_actual_cost_amt
,cast(dmos_c_valued as numeric) as dmos_valued_cost_amt
,cast(store_trans_out_u as int64) as store_trans_out_unit_cnt
,cast(store_trans_out_r as numeric) as store_trans_out_retail_amt
,cast(store_trans_out_c_actual as numeric) as store_trans_out_actual_cost_amt
,cast(store_trans_out_c_valued as numeric) as store_trans_out_valued_cost_amt
,cast(store_trans_in_u as int64) as store_trans_in_unit_cnt
,cast(store_trans_in_r as numeric) as store_trans_in_retail_amt
,cast(store_trans_in_c_actual as numeric) as store_trans_in_actual_cost_amt
,cast(store_trans_in_c_valued as numeric) as store_trans_in_valued_cost_amt
,cast(bop_alloc_notship_inv_u as int64) as bop_alloc_notship_inv_unit_cnt
,cast(bop_alloc_notship_inv_r as numeric) as bop_alloc_notship_inv_retail_amt
,cast(bop_alloc_notship_c_actual as numeric) as bop_alloc_notship_actual_cost_amt
,cast(bop_alloc_notship_c_valued as numeric) as bop_alloc_notship_valued_cost_amt
,cast(mkd_issue_u as int64) as mkd_issue_unit_cnt
,cast(mkd_issue_r as numeric) as mkd_issue_retail_amt
,cast(mkd_issue_c as numeric) as mkd_issue_cost_amt
,cast(bop_store_mkd_it_u as int64) as bop_store_mkd_it_unit_cnt
,cast(bop_store_mkd_it_r as numeric) as bop_store_mkd_it_retail_amt
,cast(bop_store_mkd_it_c_actual as numeric) as bop_store_mkd_it_actual_cost_amt
from edl_landing.ann_sku_store_inventory
!



bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --append_table --destination_table edl_stage.ann_sku_store_inventory_new --use_legacy_sql=false <<!
select c.*
from edl_stage.pre_inventory_store c
left join edl_stage.ann_sku_store_inventory_new w
    on  w.week_id = c.week_id
    where w.week_id is null
!

#cleansing and archival
bq cp --force edl_stage.pre_inventory_store edl_archive.pre_inventory_store
rc_check $? "archive copy"
bq cp --force edl_stage.ann_sku_store_inventory_new edl_stage.pre_inventory_store
rc_check $? "replace the temp table as the stage table"
bq rm --force edl_stage.ann_sku_store_inventory_new
rc_check $? "drop the temp table"


---------------------------------------------mart load ----------------

create or replace table analytic_mart_dev.pre_inventory_store
as select
 cast(chain_id as int64) as chain_id
,FARM_FINGERPRINT(cast(cast(store_id as int64) as string)) as store_key
,day_dt as  fiscal_week_dt
,FARM_FINGERPRINT(cast(cast(style_id as int64) as string)) as style_key
,FARM_FINGERPRINT(cast(cast(sku_id as int64) as string)) as item_key
,bop_store_inv_unit_cnt
,bop_store_inv_retail_amt
,bop_store_inv_actual_cost_amt
,bop_store_inv_valued_cost_amt
,bop_store_it_unit_cnt
,bop_store_it_retail_amt
,bop_store_it_actual_cost_amt
,bop_store_it_valued_cost_amt
,store_receipt_unit_cnt
,store_receipt_retail_amt
,store_receipt_actual_cost_amt
,store_receipt_valued_cost_amt
,bop_mkd_store_it_unit_cnt
,bop_mkd_store_it_retail_amt
,bop_mkd_store_it_actual_cost_amt
,bop_mkd_store_it_valued_cost_amt
,dmos_unit_cnt
,dmos_retail_amt
,dmos_actual_cost_amt
,dmos_valued_cost_amt
,store_trans_out_unit_cnt
,store_trans_out_retail_amt
,store_trans_out_actual_cost_amt
,store_trans_out_valued_cost_amt
,store_trans_in_unit_cnt
,store_trans_in_retail_amt
,store_trans_in_actual_cost_amt
,store_trans_in_valued_cost_amt
,bop_alloc_notship_inv_unit_cnt
,bop_alloc_notship_inv_retail_amt
,bop_alloc_notship_actual_cost_amt
,bop_alloc_notship_valued_cost_amt
,mkd_issue_unit_cnt
,mkd_issue_retail_amt
,mkd_issue_cost_amt
,bop_store_mkd_it_unit_cnt
,bop_store_mkd_it_retail_amt
,bop_store_mkd_it_actual_cost_amt
from edl_stage.pre_inventory_store as i
left outer join
(select fiscal_week_id
        ,day_dt
from edl_stage.pre_ann_calendar
    where fiscal_day_of_week=1) as c
on i.week_id=c.fiscal_week_id
group by 1,2,3,4,5
!


gsutil mv gs://p-asna-datasink-003/pre/sap/ANN_ACC_EDL_STORE_INVENTORY_*.txt  gs://p-asna-datasink-003/pre/archive



bq query --use_legacy_sql=false <<!
create or replace table analytic_mart_dev.pre_inventory_store
as select
 cast(chain_id as int64) as chain_id
,FARM_FINGERPRINT(cast(cast(store_id as int64) as string)) as store_key
,day_dt as  fiscal_week_dt
,FARM_FINGERPRINT(cast(cast(style_id as int64) as string)) as style_key
,FARM_FINGERPRINT(cast(cast(sku_id as int64) as string)) as item_key
,sum(bop_store_inv_unit_cnt)             as  bop_store_inv_unit_cnt
,sum(bop_store_inv_retail_amt)           as  bop_store_inv_retail_amt
,sum(bop_store_inv_actual_cost_amt)      as  bop_store_inv_actual_cost_amt
,sum(bop_store_inv_valued_cost_amt)      as  bop_store_inv_valued_cost_amt
,sum(bop_store_it_unit_cnt)              as  bop_store_it_unit_cnt
,sum(bop_store_it_retail_amt)            as  bop_store_it_retail_amt
,sum(bop_store_it_actual_cost_amt)       as  bop_store_it_actual_cost_amt
,sum(bop_store_it_valued_cost_amt)       as  bop_store_it_valued_cost_amt
,sum(store_receipt_unit_cnt)             as  store_receipt_unit_cnt
,sum(store_receipt_retail_amt)           as  store_receipt_retail_amt
,sum(store_receipt_actual_cost_amt)      as  store_receipt_actual_cost_amt
,sum(store_receipt_valued_cost_amt)      as  store_receipt_valued_cost_amt
,sum(bop_mkd_store_it_unit_cnt)          as  bop_mkd_store_it_unit_cnt
,sum(bop_mkd_store_it_retail_amt)        as  bop_mkd_store_it_retail_amt
,sum(bop_mkd_store_it_actual_cost_amt)   as  bop_mkd_store_it_actual_cost_amt
,sum(bop_mkd_store_it_valued_cost_amt)   as  bop_mkd_store_it_valued_cost_amt
,sum(dmos_unit_cnt)                      as  dmos_unit_cnt
,sum(dmos_retail_amt)                    as  dmos_retail_amt
,sum(dmos_actual_cost_amt)               as  dmos_actual_cost_amt
,sum(dmos_valued_cost_amt)               as  dmos_valued_cost_amt
,sum(store_trans_out_unit_cnt)           as  store_trans_out_unit_cnt
,sum(store_trans_out_retail_amt)         as  store_trans_out_retail_amt
,sum(store_trans_out_actual_cost_amt)    as  store_trans_out_actual_cost_amt
,sum(store_trans_out_valued_cost_amt)    as  store_trans_out_valued_cost_amt
,sum(store_trans_in_unit_cnt)            as  store_trans_in_unit_cnt
,sum(store_trans_in_retail_amt)          as  store_trans_in_retail_amt
,sum(store_trans_in_actual_cost_amt)     as  store_trans_in_actual_cost_amt
,sum(store_trans_in_valued_cost_amt)     as  store_trans_in_valued_cost_amt
,sum(bop_alloc_notship_inv_unit_cnt)     as  bop_alloc_notship_inv_unit_cnt
,sum(bop_alloc_notship_inv_retail_amt)   as  bop_alloc_notship_inv_retail_amt
,sum(bop_alloc_notship_actual_cost_amt)  as  bop_alloc_notship_actual_cost_amt
,sum(bop_alloc_notship_valued_cost_amt)  as  bop_alloc_notship_valued_cost_amt
,sum(mkd_issue_unit_cnt)                 as  mkd_issue_unit_cnt
,sum(mkd_issue_retail_amt)               as  mkd_issue_retail_amt
,sum(mkd_issue_cost_amt)                 as  mkd_issue_cost_amt
,sum(bop_store_mkd_it_unit_cnt)          as  bop_store_mkd_it_unit_cnt
,sum(bop_store_mkd_it_retail_amt)        as  bop_store_mkd_it_retail_amt
,sum(bop_store_mkd_it_actual_cost_amt)   as  bop_store_mkd_it_actual_cost_amt
from edl_stage.pre_inventory_store as i
left outer join
(select fiscal_week_id
        ,day_dt
from edl_stage.pre_ann_calendar
    where fiscal_day_of_week=1) as c
on i.week_id=c.fiscal_week_id
group by 1,2,3,4,5
!


------------------------------------
-- SELECT week_id,count(*) FROM `p-asna-analytics-002.edl_stage.plus_store_sku_inventory` where week_id = '202014'
-- group by week_id --7125056

-- SELECT week_id,count(*) FROM `p-asna-analytics-002.edl_stage.plus_hst_inventory_store_sku` where week_id = '202014'
-- group by week_id

--SELECT max(week_id),min(week_id) FROM `p-asna-analytics-002.edl_stage.plus_store_sku_inventory` --202014
--201701

-- SELECT week_id FROM `p-asna-analytics-002.edl_stage.plus_hst_inventory_store_sku` group by week_id order by week_id asc
-- week_id
-- 201947
-- 201948
-- 201949
-- 201950
-- 202015
-- 202016
-- 202017

-- SELECT count(*) FROM `p-asna-analytics-002.edl_stage.plus_hst_inventory_store_sku` --50709581

--SELECT count(*) FROM `p-asna-analytics-002.edl_stage.plus_store_sku_inventory` --1231962362

--SELECT week_id FROM `p-asna-analytics-002.edl_landing.plus_store_sku_inventory` group by week_id order by week_id asc

--SELECT count(*) FROM `p-asna-analytics-002.edl_stage.plus_store_sku_inventory_new` --50709581

--SELECT max(week_id),min(week_id) FROM `p-asna-analytics-002.edl_stage.plus_store_sku_inventory_new`

--SELECT week_id FROM `p-asna-analytics-002.edl_stage.plus_store_sku_inventory_new` group by week_id order by week_id asc

--------------------------------------

gsutil cp /data/plus/lb_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz gs://p-asna-datasink-003/plus/store_hst_inventory

bq load --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_stage/storeinventoryschema.json work.ann_sku_store_inventory \
        'gs://p-asna-datasink-003/pre/sap/ANN_ACC_EDL_STORE_INVENTORY_*.txt'
		
gsutil cp /data/plus/lb_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz gs://p-asna-datasink-003/plus/store_hst_inventory

gsutil cp /data/plus/ca_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz gs://p-asna-datasink-003/plus/store_hst_inventory



bq load --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_conform/plus_sku_store_inventory.json edl_stage.plus_hst_inventory_store_sku \  
        'gs://p-asna-datasink-003/plus/store_hst_inventory/*'


lb_ACC_DF_SKU_STORE_INVENTORY_201740_201743.txt.gz
ca_ACC_DF_SKU_STORE_INVENTORY_201740_201743.txt.gz

bq load --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_conform/plus_sku_store_inventory.json edl_stage.plus_hst_inventory_store_sku \  
        'gs://p-asna-datasink-003/plus/store_hst_inventory/*.txt.gz'


bq load --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_conform/plus_sku_store_inventory.json edl_stage.plus_inventory_store_sku_ca \
        'gs://p-asna-datasink-003/plus/store_inventory/ca_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz'


bq cp --force edl_stage.plus_inventory_store_sku_ca edl_archive.plus_inventory_store_sku_ca
bq cp --force edl_stage.plus_inventory_store_sku_lb edl_archive.plus_inventory_store_sku_lb


bq load --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_conform/plus_sku_store_inventory.json edl_stage.plus_inventory_store_sku_lb \
        'gs://p-asna-datasink-003/plus/store_inventory/lb_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz'
		


bq load --replace=false --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_conform/plus_sku_store_inventory.json edl_landing.plus_store_sku_inventory \
        'gs://p-asna-datasink-003/plus/store_inventory/ca_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz'	

bq load --replace=true --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_conform/plus_sku_store_inventory.json edl_landing.plus_store_sku_inventory \  
        'gs://p-asna-datasink-003/plus/store_inventory/*'
		
		

/* create or replace table edl_stage.plus_store_sku_inventory
as 
select * from edl_stage.plus_inventory_store_sku_ca
union all
select * from edl_stage.plus_inventory_store_sku_lb */



bq cp --force edl_stage.plus_store_sku_inventory edl_archive.plus_store_sku_inventory_backup

---------------------------------------------------------------------------------------------------------------
lb_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz
ca_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz

gsutil mv gs://p-asna-datasink-003/plus/store_inventory/*  gs://p-asna-datasink-003/plus/archive

bq load --replace=true --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_conform/plus_sku_store_inventory.json edl_landing.plus_store_sku_inventory \
        'gs://p-asna-datasink-003/plus/store_inventory/*'	

		
		
gsutil ls gs://p-asna-datasink-003/plus/ca_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz
gsutil ls gs://p-asna-datasink-003/plus/lb_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz


gsutil mv gs://p-asna-datasink-003/plus/ca_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz  gs://p-asna-datasink-003/plus/archive
gsutil mv gs://p-asna-datasink-003/plus/lb_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz  gs://p-asna-datasink-003/plus/archive
		
gsutil mv gs://p-asna-datasink-003/plus/store_hst_inventory/lb_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz  gs://p-asna-datasink-003/plus	
gsutil mv gs://p-asna-datasink-003/plus/store_hst_inventory/ca_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz  gs://p-asna-datasink-003/plus
		
------------------------------------------------------------------------------------------------------------------------
	
		


#!/usr/bin/bash

. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh


bq load --replace=true --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_conform/plus_sku_store_inventory.json edl_landing.plus_store_sku_inventory \
        'gs://p-asna-datasink-003/plus/lb_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz'


bq load --replace=false --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_conform/plus_sku_store_inventory.json edl_landing.plus_store_sku_inventory \
        'gs://p-asna-datasink-003/plus/ca_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz'


rc_check $? "creating landing table"


bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --destination_table edl_stage.plus_store_sku_inventory_new --use_legacy_sql=false <<!
select
chain_id
,store_id
,week_id
,sku_id
,style_id
,color_id
,size_id
,cast(BOP_STORE_INV_U as int64) as bop_store_inv_unit_cnt
,cast(BOP_STORE_INV_R as numeric) as bop_store_inv_retail_amt
,cast(BOP_STORE_INV_C_ACTUAL as numeric) as bop_store_inv_actual_cost_amt
,cast(BOP_STORE_INV_C_VALUED as numeric) as bop_store_inv_valued_cost_amt
,cast(BOP_STORE_IT_U as int64) as bop_store_it_unit_cnt
,cast(BOP_STORE_IT_R as numeric) as bop_store_it_retail_amt
,cast(BOP_STORE_IT_C_ACTUAL as numeric) as bop_store_it_actual_cost_amt
,cast(BOP_STORE_IT_C_VALUED as numeric) as bop_store_it_valued_cost_amt
,cast(STORE_RECEIPT_U as int64) as store_receipt_unit_cnt
,cast(STORE_RECEIPT_R as numeric) as store_receipt_retail_amt
,cast(STORE_RECEIPT_C_ACTUAL as numeric) as store_receipt_actual_cost_amt
,cast(STORE_RECEIPT_C_VALUED as numeric) as store_receipt_valued_cost_amt
,cast(BOP_MKD_STORE_IT_U as int64) as bop_mkd_store_it_unit_cnt
,cast(BOP_MKD_STORE_IT_R as numeric) as bop_mkd_store_it_retail_amt
,cast(BOP_MKD_STORE_IT_C_ACTUAL as numeric) as bop_mkd_store_it_actual_cost_amt
,cast(BOP_MKD_STORE_IT_C_VALUED as numeric) as bop_mkd_store_it_valued_cost_amt
,cast(DMOS_U as int64) as dmos_unit_cnt
,cast(DMOS_R as numeric) as dmos_retail_amt
,cast(DMOS_C_ACTUAL as numeric) as dmos_actual_cost_amt
,cast(DMOS_C_VALUED as numeric) as dmos_valued_cost_amt
,cast(STORE_TRANS_OUT_U as int64) as store_trans_out_unit_cnt
,cast(STORE_TRANS_OUT_R as numeric) as store_trans_out_retail_amt
,cast(STORE_TRANS_OUT_C_ACTUAL as numeric) as store_trans_out_actual_cost_amt
,cast(STORE_TRANS_OUT_C_VALUED as numeric) as store_trans_out_valued_cost_amt
,cast(STORE_TRANS_IN_U as int64) as store_trans_in_unit_cnt
,cast(STORE_TRANS_IN_R as numeric) as store_trans_in_retail_amt
,cast(STORE_TRANS_IN_C_ACTUAL as numeric) as store_trans_in_actual_cost_amt
,cast(STORE_TRANS_IN_C_VALUED as numeric) as store_trans_in_valued_cost_amt
,cast(BOP_ALLOC_NOTSHIP_INV_U as int64) as bop_alloc_notship_inv_unit_cnt
,cast(BOP_ALLOC_NOTSHIP_INV_R as numeric) as bop_alloc_notship_inv_retail_amt
,cast(BOP_ALLOC_NOTSHIP_C_ACTUAL as numeric) as bop_alloc_notship_actual_cost_amt
,cast(BOP_ALLOC_NOTSHIP_C_VALUED as numeric) as bop_alloc_notship_valued_cost_amt
,cast(MKD_ISSUE_U as int64) as mkd_issue_unit_cnt
,cast(MKD_ISSUE_R as numeric) as mkd_issue_retail_amt
,cast(MKD_ISSUE_C as numeric) as mkd_issue_cost_amt
from edl_landing.plus_store_sku_inventory
!

rc_check $? "creating temp table"


bq query --project_id='p-asna-analytics-002' --max_rows 1 --allow_large_results --append_table --destination_table edl_stage.plus_store_sku_inventory_new --use_legacy_sql=false <<!
SELECT
  c.*
FROM edl_stage.plus_store_sku_inventory c
WHERE c.week_id NOT IN (
  SELECT
    w.week_id
  FROM
    edl_stage.plus_store_sku_inventory_new w
    group by w.week_id
)
!

rc_check $? "append legacy records into the temp table"



#cleansing and archival
bq cp --force edl_stage.plus_store_sku_inventory edl_archive.plus_store_sku_inventory
rc_check $? "archive copy"
bq cp --force edl_stage.plus_store_sku_inventory_new edl_stage.plus_store_sku_inventory
rc_check $? "replace the temp table as the stage table"
bq rm --force edl_stage.plus_store_sku_inventory_new
rc_check $? "drop the temp table"


#move the files to pre/archive folder
gsutil mv gs://p-asna-datasink-003/plus/ca_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz  gs://p-asna-datasink-003/plus/archive
gsutil mv gs://p-asna-datasink-003/plus/lb_ACC_DF_SKU_STORE_INVENTORY_*.txt.gz  gs://p-asna-datasink-003/plus/archive

rc_check $? "move the processed file to archive folder"


