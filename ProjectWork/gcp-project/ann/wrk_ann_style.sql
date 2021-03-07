CREATE OR REPLACE TABLE work.wrk_ann_style AS
select
style_number as style_key
,style_desc as style_des
,dept as department_num
,class as class_num
,vendor_number as vendor_num
,CASE upper(intern_excl) WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as	intern_exclusive_ind   -- The null should be set to 0 or 1
,CASE upper(intern_tallstyle) WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as	intern_tallstyle_ind  -- The null should be set to 0 or 1
,markdwn_week as markdown_week_id
,exit_week as exit_week_id
,CASE upper(gift_wrap) WHEN 'Y' THEN 1 WHEN 'N' THEN 0 ELSE 0 END as gift_wrap_ind -- The null should be set to 0 or 1
,season as season_cd
,season_desc as season_des
,storeset as storeset_id
,storeset_desc as storeset_des
,merch_catg as merch_category_cd
,merch_catg_desc as merch_category_des
,purch_org as purch_org_cd
,ean_upc as ean_upc_cd
,missy_style as missy_style_id
,petite_style as petite_style_id
,tall_style as tall_style_id
,fabric_profile_value as fabric_profile_val
,fabric_profile_desc as fabric_profile_des
,silhouette_profile_value as silhouette_profile_val
,silhouette_profile_desc as silhouette_profile_des
,top_silhouette as top_silhouette_des
,neckline as neckline_des
,sleeve_len as sleeve_length_des
,novelty as novelty_des
,end_use as end_use_des
,pyramid as pyramid_des
,plus_style as plus_style_id
,batch_id
from edl_landing.ann_ann_sap_style
UNION ALL
SELECT
CAST(style_nbr AS STRING) as style_key
,style_desc as style_des
,CAST(dept_cd AS STRING) as department_num
,CAST(class_cd AS STRING) as class_num
,CAST(vendor_cd AS STRING) as vendor_num
,intern_excl_ind   as intern_exclusive_ind   -- The null should be set to 0 or 1
,intern_tallstyle_ind as intern_tallstyle_ind  -- The null should be set to 0 or 1
,CAST(markdown_week AS STRING) as markdown_week_id
,CAST(exit_week AS STRING)  as exit_week_id
,gift_wrap_ind as gift_wrap_ind -- The null should be set to 0 or 1
,CAST(season_cd AS STRING)  as season_cd
,season_desc as season_des
,NULL as storeset_id
,NULL as storeset_des
,NULL as merch_category_cd
,NULL as merch_category_des
,CAST(purch_org_cd AS STRING)  as purch_org_cd
,CAST(ean_upc AS STRING) as ean_upc_cd
,CAST(missy_style_nbr AS STRING)  as missy_style_id
,CAST(petite_style_nbr AS STRING)  as petite_style_id
,CAST(tall_style_nbr AS STRING)  as tall_style_id
,CAST(fabric_profile_cd AS STRING)  as fabric_profile_val
,fabric_profile_desc as fabric_profile_des
,CAST(silhouette_profile_cd AS STRING)  as silhouette_profile_val
,silhouette_profile_desc as silhouette_profile_des
,top_silhouette as top_silhouette_des
,neckline as neckline_des
,sleeve_len as sleeve_length_des
,novelty as novelty_des
,end_use as end_use_des
,pyramid as pyramid_des
,CAST(plus_style_nbr AS STRING)  as plus_style_id
,batch_id
FROM edl_landing.ann_hst_lu_style_vw
WHERE style_nbr NOT IN (SELECT CAST(style_number as INT64) FROM edl_landing.ann_ann_sap_style)
;
