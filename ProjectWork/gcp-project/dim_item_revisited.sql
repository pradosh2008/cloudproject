--/*********************************************************************
--* 
--* Script Name: DIM_ITEM table creation and data population
--*
--**********************************************************************/
--/*--
--item_key:md5(concat('JUS',short_sku_id))
--If short_sku_id is not null by merchandise_style_id, let me know.
--We might have to use an analytic function to grab the latest one
--*/

set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;

DROP TABLE IF EXISTS ascena_analytic_mart_dev.WORK_DIM_ITEM 
CREATE TABLE IF NOT EXISTS ascena_analytic_mart_dev.WORK_DIM_ITEM
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

DROP TABLE IF EXISTS ascena_analytic_mart_dev.tran_merch_item;
--------------------------------------Items matching in transaction and merchandise table-----------------
CREATE TABLE ascena_analytic_mart_dev.tran_merch_item
stored as orc
TBLPROPERTIES ("orc.compress"="SNAPPY")
as
SELECT short_sku_no, merchandise_style_id, department_no, class_no, style_no, group_no, style_desc,
last_update_date, last_update_job_id, dw_active, batch_id  
FROM (SELECT
td.short_sku_no,
td.merchandise_style_id, 
td.department_no, 
td.class_no, 
td.style_no,
ms.group_no,
ms.style_desc,
td.last_update_date,
td.last_update_job_id,
ms.dw_active,
td.batch_id,
row_number() over (partition by td.short_sku_no 
order by from_unixtime(UNIX_TIMESTAMP(td.transaction_date, "MM/dd/yyyy HH:mm:ss")) desc
) as row_num
FROM ascena_staging_dev.jus_hst_transaction_details_dr as td
LEFT OUTER JOIN ascena_staging.jus_hst_merchandise_styles as ms ON (td.merchandise_style_id = ms.merchandise_style_id)
) AS A where row_num = 1
GROUP BY short_sku_no, merchandise_style_id, department_no, class_no, style_no, group_no, style_desc,
last_update_date, last_update_job_id, dw_active, batch_id;

--------------------------------------Unique and Latest GROUP_NO AND GROUP_DESC FROM merchandise table-----------------
DROP TABLE IF EXISTS ascena_analytic_mart_dev.temp_group;
DROP TABLE IF EXISTS ascena_analytic_mart_dev.temp_dept;
DROP TABLE IF EXISTS ascena_analytic_mart_dev.temp_class;
DROP TABLE IF EXISTS ascena_analytic_mart_dev.temp_style;

CREATE TABLE ascena_analytic_mart_dev.temp_group
stored as orc
TBLPROPERTIES ("orc.compress"="SNAPPY")
as
SELECT GROUP_NO, GROUP_DESC, DIVISION_DESC, DIVISION_CODE FROM (SELECT group_no, group_desc, division_desc, division_code, row_number() over (partition by group_no order by end_effective_date desc) as rn
FROM ascena_staging.jus_hst_merchandise_styles
) AS A
WHERE rn = 1
;
--------------------------------------Unique and Latest DEPARTMENT_NO AND DEPT_DESC FROM merchandise table-----------------
CREATE TABLE ascena_analytic_mart_dev.temp_dept
stored as orc
TBLPROPERTIES ("orc.compress"="SNAPPY")
as
SELECT CAST(DEPARTMENT_NO AS INT) AS DEPARTMENT_NO, DEPARTMENT_DESC FROM (SELECT DEPARTMENT_NO, DEPARTMENT_DESC 
FROM (SELECT CAST(department_no as STRING) as department_no, department_desc, row_number() over (partition by department_no order by end_effective_date desc) as rn
FROM ascena_staging.jus_hst_merchandise_styles
) AS A
WHERE rn = 1
) AS DEPT
GROUP BY DEPARTMENT_NO, DEPARTMENT_DESC;

--------------------------------------Unique and Latest CLASS_NO AND CLASS_DESC FROM merchandise table-----------------
CREATE TABLE ascena_analytic_mart_dev.temp_class
stored as orc
TBLPROPERTIES ("orc.compress"="SNAPPY")
as
SELECT CLASS_NO, CLASS_DESC FROM (
SELECT CLASS_NO, CLASS_DESC FROM (SELECT class_no, class_desc, row_number() over (partition by class_no order by end_effective_date desc) as rn
FROM ascena_staging.jus_hst_merchandise_styles
) AS A
WHERE rn = 1
UNION
SELECT CLASS_NUM as CLASS_NO, TRIM(SUBSTR(CLASS_NM, 0, 25)) as CLASS_DESC
FROM (SELECT jus_header
, CAST(REGEXP_EXTRACT(trim(jus_detail), '[0-9]{8}',0) as INT) as CLASS_NUM
, REGEXP_EXTRACT(trim(jus_detail), '[0-9]{12}',0) as DEPT_NUM
, TRIM(SUBSTR(jus_detail, 13)) as CLASS_NM
FROM ascena_staging.jus_twe_all_merchandise
WHERE jus_header = 'TWECL') AS TWECL
WHERE CLASS_NUM NOT IN (SELECT DISTINCT CLASS_NO FROM ascena_staging.jus_hst_merchandise_styles)
) AS A
GROUP BY CLASS_NO, CLASS_DESC;

--------------------------------------Unique and Latest STYLE_NO AND related details FROM merchandise table-----------------
CREATE TABLE ascena_analytic_mart_dev.temp_style
stored as orc
TBLPROPERTIES ("orc.compress"="SNAPPY")
as
SELECT group_no, class_no, department_no, style_no, style_desc, season_code FROM
(SELECT group_no, class_no, department_no, style_no, style_desc, season_code
, row_number() over (partition by group_no, department_no, class_no, style_no, season_code order by end_effective_date desc) as rn
, dw_active
FROM ascena_staging.jus_hst_merchandise_styles
) AS A
WHERE rn = 1 AND dw_active ='Y';

--------------------------------------DIM_ITEM_MERCH Table Creation & Population Script--------------------------------------
DROP TABLE ascena_analytic_mart_dev.DIM_ITEM_MERCH

CREATE TABLE ascena_analytic_mart_dev.DIM_ITEM_MERCH ( 
ITEM_KEY CHAR(32)
, BRAND_CD VARCHAR(3)
, SHORT_SKU_ID BIGINT
, GROUP_NUM SMALLINT
, DEPARTMENT_NUM SMALLINT
, CLASS_NUM SMALLINT
, STYLE_NUM SMALLINT
, COLOR_NUM SMALLINT
, SIZE_NUM SMALLINT
, LONG_SKU_ID STRING
, GROUP_DES STRING
, DEPARTMENT_DES STRING
, CLASS_DES STRING
, STYLE_DES STRING
, DIVISION_DES STRING
, SEASON_CD VARCHAR(3)
, DIVISION_CD VARCHAR(3)
, SEASON_DES STRING
, SEASON_LONG_DES STRING
, PRIME_SILHO_DES STRING
, SEC_SILHUET_DES STRING
, COLOR_FAM_DES STRING
, DISSECTION_DES STRING
, FABRIC_DES STRING
, GRAPH_CONT_DES STRING
, PATTERN_DES STRING
, FLOORSET_DES STRING
, VIS_MOV_DATE_DES STRING
	--,merchandise_style_id INT
    --,start_effective_dt date
    --,end_effective_dt date
	--,create_dt date
    --,create_job_id INT
    --,last_update_tms TIMESTAMP
    --,last_update_job_id INT
    --,merchandise_style_active_ind tinyint
    --,batch_id BIGINT
) 
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy");

INSERT OVERWRITE TABLE ascena_analytic_mart_dev.DIM_ITEM_MERCH
SELECT MD5(CONCAT('JUS|', COALESCE(SHORT_SKU_ID, ''))) AS ITEM_KEY, BRAND_CD, SHORT_SKU_ID, GROUP_NUM, DEPARTMENT_NUM, CLASS_NUM, STYLE_NUM, COLOR_NUM, SIZE_NUM, LONG_SKU_ID
, GROUP_DES, DEPARTMENT_DES, CLASS_DES, STYLE_DES, DIVISION_DES, SEASON_CD, DIVISION_CD
, SEASON_DES, SEASON_LONG_DES, PRIME_SILHO_DES, SEC_SILHUET_DES, COLOR_FAM_DES, DISSECTION_DES
, FABRIC_DES, GRAPH_CONT_DES, PATTERN_DES, FLOORSET_DES, VIS_MOV_DATE_DES
FROM (
SELECT G.BRAND_CD, SUBSTR(G.DIVSION_DESC, 4) AS DIVISION_DES, G.BRAND_CD as DIVISION_CD, CAST(G.GROUP_NO AS SMALLINT) AS GROUP_NUM, G.GROUP_DESC as GROUP_DES
, CAST(D.DEPT_NO AS SMALLINT) AS DEPARTMENT_NUM, D.DEPT_DESC AS DEPARTMENT_DES, CAST(C.CLASS_NO AS SMALLINT) AS CLASS_NUM
, C.CLASS_DESC AS CLASS_DES, CAST(S.STYLE_NO AS SMALLINT) AS STYLE_NUM, S.STYLE_DESC AS STYLE_DES, S.SEASON_DESC AS SEASON_DES, S.SEASON_CD
, A.LONG_SKU AS LONG_SKU_ID, CAST(A.COLOR_NO AS SMALLINT) AS COLOR_NUM, CAST(A.SIZE_NO AS SMALLINT) AS SIZE_NUM, CAST(A.SHORT_SKU AS BIGINT) AS SHORT_SKU_ID
, A.SEASON AS SEASON_LONG_DES, A.PRIME_SILHO AS PRIME_SILHO_DES, A.SEC_SILHUET AS SEC_SILHUET_DES, A.COLOR_FAM AS COLOR_FAM_DES, A.DISSECTION AS DISSECTION_DES
, A.FABRIC AS FABRIC_DES, A.GRAPH_CONT AS GRAPH_CONT_DES, A.PATTERN AS PATTERN_DES, A.FLOORSET AS FLOORSET_DES, A.VIS_MOV_DATE AS VIS_MOV_DATE_DES
 FROM 
 ( 	
					SELECT jus_header, TRIM(SUBSTR(GROUP_NUM, 3,2)) as GROUP_NO, GROUP_DESC, TRIM(SUBSTR(DIVISION_NM, -11)) as DIVSION_DESC, REGEXP_EXTRACT(TRIM(SUBSTR(DIVISION_NM, -11)), '[A-Z]{3}', 0) AS BRAND_CD 
					FROM (
					 SELECT jus_header
					 , REGEXP_EXTRACT(trim(jus_detail), '[0-9]{4}',0) as GROUP_NUM
					 , TRIM(SUBSTR(jus_detail, 5, 25)) as GROUP_DESC
					 , TRIM(REGEXP_EXTRACT(trim(jus_detail),'[A-Z](.*?)[ ]*$', 0)) as DIVISION_NM
					 FROM ascena_staging_dev.jus_twe_all_merchandise
					 WHERE jus_header = 'TWEGP') AS TWEGP) AS G 
 LEFT OUTER JOIN 
 ( 
					SELECT jus_header, TRIM(SUBSTR(DEPT_NUM, 3, 2)) as DEPT_NO, DEPT_DESC, SUBSTR(TRIM(SUBSTR(GROUP_NUM, -11)), 3, 2) as GROUP_NO 
					FROM (
					 SELECT jus_header
					 , REGEXP_EXTRACT(trim(jus_detail), '[0-9]*',0) as DEPT_NUM
					 , TRIM(SUBSTR(jus_detail, 5, 25)) as DEPT_DESC
					 , TRIM(REGEXP_EXTRACT(trim(jus_detail),'[A-Z](.*?)[ ]*$', 0)) as GROUP_NUM
					 FROM ascena_staging_dev.jus_twe_all_merchandise
					 WHERE jus_header = 'TWEDP') AS TWEDP) AS D ON G.GROUP_NO = D.GROUP_NO
  LEFT OUTER JOIN
 (
					 SELECT JUS_HEADER, STYLE_NUM AS STYLE_NO, STYLE_NM AS STYLE_DESC, DEPT_NUM AS DEPT_NO, CAST(SUBSTR(CLASS_NUM, -4) AS INT) AS CLASS_NO, SEASON_CD, SEASON_DESC 
					 FROM (
					 SELECT jus_header
					 , CAST(SUBSTR(jus_detail, 0, 4) AS INT) as STYLE_NUM
					 , TRIM(REVERSE(SUBSTR(reverse(TRIM(SUBSTR(jus_detail, 5))), 14))) as STYLE_NM
					 , CAST(TRIM(SUBSTR(jus_detail, -14, 5)) AS INT) as DEPT_NUM
					 , REGEXP_EXTRACT(TRIM(SUBSTR(jus_detail, -14)), '[0-9]*', 0) as CLASS_NUM
					 , TRIM(SUBSTR(jus_detail, -1)) as SEASON_CD
					 , CASE TRIM(SUBSTR(jus_detail, -1)) WHEN 'F' THEN 'FALL' WHEN 'S' THEN 'SPRING' ELSE 'NONE' END as SEASON_DESC
					 FROM ascena_staging_dev.jus_twe_all_merchandise
					 WHERE jus_header = 'TWESY') AS TWESY) AS S ON S.DEPT_NO = D.DEPT_NO
LEFT OUTER JOIN
 (  
					SELECT jus_header, CLASS_NUM as CLASS_NO, TRIM(SUBSTR(DEPT_NUM,-2)) as DEPT_NO, TRIM(SUBSTR(CLASS_NM, 0, 25)) as CLASS_DESC 
					FROM (
					 SELECT jus_header
					 , CAST(REGEXP_EXTRACT(trim(jus_detail), '[0-9]{8}',0) as INT) as CLASS_NUM
					 , REGEXP_EXTRACT(trim(jus_detail), '[0-9]{12}',0) as DEPT_NUM
					 , TRIM(SUBSTR(jus_detail, 13)) as CLASS_NM
					 FROM ascena_staging.jus_twe_all_merchandise
					 WHERE jus_header = 'TWECL') AS TWECL) AS C ON C.CLASS_NO = S.CLASS_NO
LEFT OUTER JOIN
(
					SELECT jus_header, LONG_SKU
					 , CAST(SUBSTR(LONG_SKU, 1, 3) AS INT) as dept_no, CAST(SUBSTR(LONG_SKU, 4, 4) AS INT) as class_no
					 , CAST(SUBSTR(LONG_SKU, 13, 4) AS INT) as style_no, CAST(SUBSTR(LONG_SKU, 17, 3) AS INT) as color_no
					 , CAST(SUBSTR(LONG_SKU, 21, 3) AS INT) as size_no
					 , SUBSTR(LONG_SKU, -8) as SHORT_SKU
					 , TRIM(REGEXP_EXTRACT(TRIM(TEXT_S), 'Season(.*?)\PrimeSilho', 1)) as Season
					 , TRIM(REGEXP_EXTRACT(TRIM(TEXT_S), 'PrimeSilho(.*?)\SecSilhuet', 1)) as Prime_Silho
					 , TRIM(REGEXP_EXTRACT(TRIM(TEXT_S), 'SecSilhuet(.*?)\ColorFam', 1)) as Sec_Silhuet
					 , TRIM(REGEXP_EXTRACT(TRIM(TEXT_S), 'ColorFam(.*?)\Dissection', 1)) as Color_Fam
					 , TRIM(REGEXP_EXTRACT(TRIM(TEXT_S), 'Dissection(.*?)\Fabric', 1)) as Dissection
					 , TRIM(REGEXP_EXTRACT(TRIM(TEXT_S), 'Fabric(.*?)\GraphCont', 1)) as Fabric
					 , TRIM(REGEXP_EXTRACT(TRIM(TEXT_S), 'GraphCont(.*?)\Pattern', 1)) as Graph_Cont
					 , TRIM(REGEXP_EXTRACT(TRIM(TEXT_S), 'Pattern(.*?)\Floorset', 1)) as Pattern
					 , TRIM(REGEXP_EXTRACT(TRIM(TEXT_S), 'Floorset(.*?)\VisMovDate', 1)) as Floorset
					 , TRIM(SUBSTR(REGEXP_EXTRACT(TRIM(TEXT_S), 'VisMovDate(.*?)$', 0), 11)) as Vis_Mov_Date
					 FROM (
					  SELECT jus_header
					 , REGEXP_EXTRACT(TRIM(jus_detail), '[0-9]*', 0) as LONG_SKU
					 , REGEXP_EXTRACT(TRIM(jus_detail), '[A-Z](.*?)[ ]*$', 0) as TEXT_S
					 FROM ascena_staging_dev.jus_twe_all_merchandise 
					 WHERE jus_header = 'TWEAT') AS TWEAT) AS A
					 ON A.DEPT_NO = D.DEPT_NO AND A.STYLE_NO = S.STYLE_NO AND A.CLASS_NO = C.CLASS_NO
WHERE A.LONG_SKU IS NOT NULL
 ) AS MERCH_TAB
 GROUP BY BRAND_CD, SHORT_SKU_ID, GROUP_NUM, DEPARTMENT_NUM, CLASS_NUM, STYLE_NUM, COLOR_NUM, SIZE_NUM, LONG_SKU_ID
, GROUP_DES, DEPARTMENT_DES, CLASS_DES, STYLE_DES, DIVISION_DES, SEASON_CD, DIVISION_CD
, SEASON_DES, SEASON_LONG_DES, PRIME_SILHO_DES, SEC_SILHUET_DES, COLOR_FAM_DES, DISSECTION_DES
, FABRIC_DES, GRAPH_CONT_DES, PATTERN_DES, FLOORSET_DES, VIS_MOV_DATE_DES;

--------------------------------------INSERTING DATA INTO DIM_ITEM TABLE WITH LATEST ----------------------------------------
INSERT OVERWRITE TABLE ascena_analytic_mart_dev.WORK_DIM_ITEM
SELECT item_key, brand_cd, short_sku_id, MERCHANDISE_STYLE_ID, CLASS_NUM, STYLE_NUM, DEPARTMENT_NUM, GROUP_NUM, DIVISION_CD
, SEASON_CD, STYLE_DES, CLASS_DES, DEPARTMENT_DES, GROUP_DES, DIVISION_DES, LAST_UPDATE_TMS, LAST_UPDATE_JOB_ID, MERCHANDISE_STYLE_ACTIVE_IND, BATCH_ID FROM 
(SELECT md5(concat('JUS|', coalesce(ti.short_sku_no,''))) as ITEM_KEY
, 'JUS' as BRAND_CD
, ti.short_sku_no as SHORT_SKU_ID
, ti.merchandise_style_id as MERCHANDISE_STYLE_ID
, tc.class_no as CLASS_NUM
, ti.style_no as STYLE_NUM
, td.department_no as DEPARTMENT_NUM
, tg.group_no as GROUP_NUM
, tg.division_code as DIVISION_CD
, ts.season_code as SEASON_CD
--, transaction_date
, ti.style_desc as STYLE_DES
, tc.class_desc as CLASS_DES
, td.department_desc as DEPARTMENT_DES
, tg.group_desc as GROUP_DES
, tg.division_desc as DIVISION_DES
, from_unixtime(UNIX_TIMESTAMP(ti.last_update_date, "MM/dd/yyyy HH:mm:ss")) as LAST_UPDATE_TMS
, ti.last_update_job_id as LAST_UPDATE_JOB_ID
, case when ti.dw_active = 'Y' then 1 else 0 end as MERCHANDISE_STYLE_ACTIVE_IND
, ti.batch_id as BATCH_ID
FROM ascena_analytic_mart_dev.tran_merch_item as ti
LEFT OUTER JOIN ascena_analytic_mart_dev.temp_dept as td ON (ti.department_no = td.department_no)
LEFT OUTER JOIN ascena_analytic_mart_dev.temp_group as tg ON (ti.group_no = tg.group_no)
LEFT OUTER JOIN ascena_analytic_mart_dev.temp_class as tc ON (ti.class_no = tc.class_no)
LEFT OUTER JOIN ascena_analytic_mart_dev.temp_style as ts ON (ti.style_no = ts.style_no AND TRIM(ti.style_desc) = TRIM(ts.style_desc) AND ti.group_no = ts.group_no AND ti.class_no = ts.class_no)
UNION
SELECT item_key, brand_cd, short_sku_id, merchandise_style_id, class_num, style_num, department_num,
group_num, division_cd, season_cd, style_des, class_des,department_des, group_des, division_des,
NULL AS last_update_tms, NULL AS last_update_job_id, NULL AS merchandise_style_active_ind, 20180101 AS batch_id
FROM
(SELECT MD5(CONCAT('JUS|', COALESCE(im.SHORT_SKU_ID, ''))) AS ITEM_KEY
, im.BRAND_CD
, im.SHORT_SKU_ID
, tdd.merchandise_style_id
, im.CLASS_NUM
, im.STYLE_NUM
, im.DEPARTMENT_NUM
, im.GROUP_NUM
, im.DIVISION_CD
, im.SEASON_CD
, im.STYLE_DES
, tc.CLASS_DESC as CLASS_DES
, td.DEPARTMENT_DESC AS DEPARTMENT_DES
, tg.GROUP_DESC AS GROUP_DES
, im.DIVISION_DES
FROM ascena_analytic_mart_dev.DIM_ITEM_MERCH as im 
left join ascena_staging_dev.jus_hst_transaction_details_dr as tdd
on im.SHORT_SKU_ID = tdd.short_sku_no
left outer join ascena_analytic_mart_dev.temp_class as tc ON (im.CLASS_NUM = tc.class_no)
left outer join ascena_analytic_mart_dev.temp_dept as td ON (im.DEPARTMENT_NUM = td.department_no)
left outer join ascena_analytic_mart_dev.temp_group as tg ON (im.GROUP_NUM = tg.group_no)
where tdd.short_sku_no is null
group by
MD5(CONCAT('JUS|', COALESCE(im.SHORT_SKU_ID, '')))
, im.BRAND_CD
, im.SHORT_SKU_ID
, tdd.merchandise_style_id
, im.CLASS_NUM
, im.STYLE_NUM
, im.DEPARTMENT_NUM
, im.GROUP_NUM
, im.DIVISION_CD
, im.SEASON_CD
, im.STYLE_DES
, tc.CLASS_DESC
, td.DEPARTMENT_DESC
, tg.GROUP_DESC
, im.DIVISION_DES) AS ITEM_MERCH) AS TAB
 GROUP BY item_key, brand_cd, short_sku_id, MERCHANDISE_STYLE_ID, CLASS_NUM, STYLE_NUM, DEPARTMENT_NUM, GROUP_NUM, DIVISION_CD
, SEASON_CD, STYLE_DES, CLASS_DES, DEPARTMENT_DES, GROUP_DES, DIVISION_DES, LAST_UPDATE_TMS, LAST_UPDATE_JOB_ID, MERCHANDISE_STYLE_ACTIVE_IND, BATCH_ID;

-------------------------------------------------------DROP STATEMENTS ------------------------------------------BE CAREFUL...!!!
DROP TABLE IF EXISTS ascena_analytic_mart_dev.temp_group;
DROP TABLE IF EXISTS ascena_analytic_mart_dev.temp_dept;
DROP TABLE IF EXISTS ascena_analytic_mart_dev.temp_class;
DROP TABLE IF EXISTS ascena_analytic_mart_dev.temp_style;
-------------------------------------------------------DROP STATEMENTS ------------------------------------------END...!!!