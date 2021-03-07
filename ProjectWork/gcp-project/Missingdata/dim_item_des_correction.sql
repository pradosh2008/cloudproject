set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;

drop table ascena_analytic_mart_dev.DIM_ITEM_DES_CORRECTION;
CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.DIM_ITEM_DES_CORRECTION 
(
     item_key  char(32)
    ,brand_cd               string
    ,short_sku_id INT
    ,merchandise_style_id INT
    ,class_num INT
    ,style_num INT
    ,department_num INT
    ,group_num INT
    --,start_effective_dt date
    --,end_effective_dt date
    ,division_cd  string
    ,season_cd  string
    ,style_des  string
    ,class_des  string
    ,department_des  string
    ,group_des  string
    ,division_des  string
    --,create_dt date
    --,create_job_id INT
    ,last_update_tms TIMESTAMP
    ,last_update_job_id INT
    ,merchandise_style_active_ind tinyint
    ,batch_id BIGINT
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;

INSERT OVERWRITE TABLE ascena_analytic_mart_dev.DIM_ITEM_DES_CORRECTION
select
 item_key
, brand_cd
, short_sku_id
, merchandise_style_id
, tsd.class_num
, style_num
, tded.department_num
, tgd.group_num
, tdid.division_cd
, season_cd
, style_des
, tsd.class_des
, tded.department_des
, tgd.group_des
, tdid.division_des
, last_update_tms
, last_update_job_id
, merchandise_style_active_ind
, batch_id
from (SELECT md5(concat('JUS|', coalesce(short_sku_id,''))) as item_key
    ,brand_cd as brand_cd
    ,short_sku_id as short_sku_id
    ,merchandise_style_id as merchandise_style_id
    ,class_no as class_num
    ,style_no as style_num
    ,department_no as department_num
    ,group_no as group_num
    ,division_cd as division_cd
    ,season_cd as season_cd
    ,style_desc as style_des
    ,class_desc as class_des
    ,department_desc as department_des 
    ,group_desc as group_des
    ,division_desc as division_des
    ,last_update_tms
    ,last_update_job_id as last_update_job_id
    ,dw_active as merchandise_style_active_ind
    ,batch_id as batch_id
FROM (SELECT 'JUS' as brand_cd
        ,td.short_sku_no as short_sku_id
        ,td.merchandise_style_id as merchandise_style_id
        ,td.class_no
        ,td.style_no
        ,td.department_no
        ,ms.group_no
        --,to_date(from_unixtime(UNIX_TIMESTAMP(ms.start_effective_date, "MM/dd/yyyy"))) as start_effective_dt
        --,to_date(from_unixtime(UNIX_TIMESTAMP(ms.end_effective_date, "MM/dd/yyyy"))) as end_effective_dt
        ,ms.division_code as division_cd
        ,ms.season_code as season_cd
        ,ms.style_desc
        ,ms.class_desc
        ,ms.department_desc
        ,ms.group_desc
        ,ms.division_desc
        --,to_date(from_unixtime(UNIX_TIMESTAMP(ms.create_date , "MM/dd/yyyy"))) as create_dt
        ,from_unixtime(UNIX_TIMESTAMP(td.last_update_date, "MM/dd/yyyy HH:mm:ss")) as last_update_tms
        ,td.last_update_job_id
        ,case when ms.dw_active = 'Y' then 1 else 0 end                 as dw_active
        ,td.batch_id
        ,row_number() over (partition by td.short_sku_no 
                            order by case when ms.merchandise_style_id is null
                                            then 1 else 0
                                    end
                                    ,from_unixtime(UNIX_TIMESTAMP(td.last_update_date, "MM/dd/yyyy HH:mm:ss")) desc
                        )                                                   as row_num
    FROM ascena_staging_dev.jus_hst_transaction_details_dr as td
    left JOIN ascena_staging.jus_hst_merchandise_styles as ms 
        ON (ms.merchandise_style_id = td.merchandise_style_id) 
) as A 
where row_num = 1
union
SELECT MD5(CONCAT('JUS|', COALESCE(SHORT_SKU_ID, ''))) AS ITEM_KEY
, BRAND_CD
, SHORT_SKU_ID
, null as merchandise_style_id
, CLASS_NUM
, STYLE_NUM
, DEPARTMENT_NUM
, GROUP_NUM
, DIVISION_CD
, SEASON_CD
, STYLE_DES
, CLASS_DES
, DEPARTMENT_DES
, GROUP_DES
, DIVISION_DES
,NULL AS last_update_tms 
,NULL AS last_update_job_id 
,NULL AS merchandise_style_active_ind 
,20180101 AS batch_id
FROM ascena_analytic_mart_dev.DIM_ITEM_MERCH as im 
left join ascena_staging_dev.jus_hst_transaction_details_dr as tdd
on im.SHORT_SKU_ID=tdd.short_sku_no
where tdd.short_sku_no is null
group by
MD5(CONCAT('JUS|', COALESCE(SHORT_SKU_ID, '')))
, BRAND_CD
, SHORT_SKU_ID
, CLASS_NUM
, STYLE_NUM
, DEPARTMENT_NUM
, GROUP_NUM
, DIVISION_CD
, SEASON_CD
, STYLE_DES
, CLASS_DES
, DEPARTMENT_DES
, GROUP_DES
, DIVISION_DES
) as tbl
left outer join ascena_analytic_mart_dev.tmp_class_des as tsd on COALESCE(trim(tbl.class_des), '')=COALESCE(trim(tsd.class_des), '')
left outer join ascena_analytic_mart_dev.tmp_DIVISION_des as tdid on COALESCE(trim(tbl.DIVISION_DES), '')=COALESCE(trim(tdid.DIVISION_DES), '')
left outer join ascena_analytic_mart_dev.tmp_DEPARTMENT_DES as tded on COALESCE(trim(tbl.DEPARTMENT_DES), '')=COALESCE(trim(tded.DEPARTMENT_DES), '')
left outer join ascena_analytic_mart_dev.tmp_group_des as tgd on COALESCE(trim(tbl.GROUP_DES), '')=COALESCE(trim(tgd.GROUP_DES), '');


