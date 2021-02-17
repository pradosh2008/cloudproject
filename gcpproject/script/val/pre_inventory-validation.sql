SELECT sum(case when b.style_key is null then 0 else 1 end)    as match_count
,sum(case when b.style_key is null then 1 else 0 end)     as orphan_count
FROM `p-asna-analytics-002.analytic_mart.pre_store_inventory`  a
LEFT OUTER JOIN analytic_mart.pre_style b
on a.style_key = b.style_key


select count(distinct a.style_key) from 
`p-asna-analytics-002.analytic_mart.pre_store_inventory`  a
LEFT OUTER JOIN analytic_mart.pre_style b
on a.style_key = b.style_key
where b.style_key is null



SELECT sum(case when b.item_key is null then 0 else 1 end)    as match_count
,sum(case when b.item_key is null then 1 else 0 end)     as orphan_count
FROM `p-asna-analytics-002.analytic_mart.pre_store_inventory`  a
LEFT OUTER JOIN analytic_mart.pre_item b
on a.item_key = b.item_key


select count(distinct a.item_key) from 
`p-asna-analytics-002.analytic_mart.pre_store_inventory`  a
LEFT OUTER JOIN analytic_mart.pre_item b
on a.item_key = b.item_key
where b.item_key is null


SELECT sum(case when b.store_key is null then 0 else 1 end)    as match_count
,sum(case when b.store_key is null then 1 else 0 end)     as orphan_count
FROM `p-asna-analytics-002.analytic_mart.pre_store_inventory`  a
LEFT OUTER JOIN analytic_mart.pre_store b
on a.store_key = b.store_key