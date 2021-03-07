CREATE or replace table work.pre_style
as
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
from edl_landing.ann_ann_sap_style;