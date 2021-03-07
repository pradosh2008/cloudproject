SELECT 
 sum(case when pi.sku_id is null then 0 else 1 end)   as match_count
,sum(case when pi.sku_id is null then 1 else 0 end)     as orphan_count
from p-asna-analytics-002.work.csm_loft_product ep
left outer join
(select 
  sku_id 
  from p-ascn-da-aadp-001.analytic_mart.pre_item 
  group by sku_id) pi
on ep.sku=pi.sku_id


select count(*)
from (select 
      sku
      from
      p-asna-analytics-002.work.csm_loft_product
      group by 1) t --3377516
      
select count(*)
from (select 
      sku_id
      from
      p-ascn-da-aadp-001.analytic_mart.pre_item
      group by 1) t --3377981



select
sum(case when pi.department_cd				=ep.DEPT_CD       then 1 else 0 end) as match_dept_count
,sum(case when pi.department_cd				=ep.DEPT_CD       then 0 else 1 end) as mismatch_dept_count
,sum(case when cast(concat(pi.department_cd,pi.class_cd)	as  int64)				=ep.CLASS_CD      then 1 else 0 end) as match_class_count
,sum(case when cast(concat(pi.department_cd,pi.class_cd)	as int64)				=ep.CLASS_CD      then 0 else 1 end) as mismatch_class_count
,sum(case when pi.style_id					=ep.STYLE_NBR     then 1 else 0 end) as match_style_count
,sum(case when pi.style_id					=ep.STYLE_NBR     then 0 else 1 end) as mismatch_style_count
,sum(case when pi.color_cd					=ep.COLOR_CD      then 1 else 0 end) as match_color_count
,sum(case when pi.color_cd					=ep.COLOR_CD      then 0 else 1 end) as mismatch_color_count
,sum(case when pi.size_cd					=ep.SIZE_CD       then 1 else 0 end) as match_size_count
,sum(case when pi.size_cd					=ep.SIZE_CD       then 0 else 1 end) as mismatch_size_count
,sum(case when pi.department_category_cd	=ep.DEPT_CATEGORY_CD  then 1 else 0 end) as match_dept_category_count
,sum(case when pi.department_category_cd	=ep.DEPT_CATEGORY_CD  then 0 else 1 end) as mismatch_dept_category_count
,sum(case when pi.merch_category_cd		=ep.MERCH_CATEGORY_CD then 1 else 0 end) as match_merch_category_count
,sum(case when pi.merch_category_cd		=ep.MERCH_CATEGORY_CD then 0 else 1 end) as mismatch_merch_category_count
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id


select
sum(case when coalesce(pi.department_cd,0)	=coalesce(cast(ep.DEPT_CD as int64),0)  then 1 else 0 end) as match_dept_count
,sum(case when coalesce(pi.department_cd,0)	=coalesce(cast(ep.DEPT_CD as int64),0)  then 0 else 1 end) as mismatch_dept_count
,sum(case when concat(pi.department_cd,pi.class_cd)				=ep.CLASS_CD      then 1 else 0 end) as match_class_count
,sum(case when concat(pi.department_cd,pi.class_cd)				=ep.CLASS_CD      then 0 else 1 end) as mismatch_class_count
,sum(case when pi.style_id					=cast(ep.STYLE_NBR as int64)    then 1 else 0 end) as match_style_count
,sum(case when pi.style_id					=cast(ep.STYLE_NBR as int64)     then 0 else 1 end) as mismatch_style_count
,sum(case when pi.color_cd					=cast(ep.COLOR_CD as int64)      then 1 else 0 end) as match_color_count
,sum(case when pi.color_cd					=cast(ep.COLOR_CD as int64)     then 0 else 1 end) as mismatch_color_count
,sum(case when pi.size_cd					=cast(ep.SIZE_CD  as int64)    then 1 else 0 end) as match_size_count
,sum(case when pi.size_cd					=cast(ep.SIZE_CD  as int64)       then 0 else 1 end) as mismatch_size_count
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id




select
sum(case when pi.department_cd				=ep.DEPT_CD       then 1 else 0 end) as match_dept_count
,sum(case when pi.department_cd				=ep.DEPT_CD       then 0 else 1 end) as mismatch_dept_count
,sum(case when cast(concat(pi.department_cd,case when length(trim(pi.class_cd))=5 then concat('0',pi.class_cd) else pi.class_cd  end) as int64)=ep.CLASS_CD then 1 else 0 end) as match_class_count
,sum(case when cast(concat(pi.department_cd,case when length(trim(pi.class_cd))=5 then concat('0',pi.class_cd) else pi.class_cd  end) as int64)=ep.CLASS_CD then 0 else 1 end) as mismatch_class_count
,sum(case when pi.style_id					=ep.STYLE_NBR     then 1 else 0 end) as match_style_count
,sum(case when pi.style_id					=ep.STYLE_NBR     then 0 else 1 end) as mismatch_style_count
,sum(case when pi.color_cd					=ep.COLOR_CD      then 1 else 0 end) as match_color_count
,sum(case when pi.color_cd					=ep.COLOR_CD      then 0 else 1 end) as mismatch_color_count
,sum(case when pi.size_cd					=ep.SIZE_CD       then 1 else 0 end) as match_size_count
,sum(case when pi.size_cd					=ep.SIZE_CD       then 0 else 1 end) as mismatch_size_count
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id




select
 pi.department_cd prod_dept
,pi.class_cd prod_class
,cast(concat(pi.department_cd,case when length(trim(pi.class_cd))=5 then concat('0',pi.class_cd) else pi.class_cd  end) as int64)
,ep.CLASS_CD  epsilon_class
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
where cast(concat(pi.department_cd,case when length(trim(pi.class_cd))=5 then concat('0',pi.class_cd) else pi.class_cd  end) as int64) !=ep.CLASS_CD

select
pi.department_cd,
pi.class_cd,
cast(concat(pi.department_cd,case when length(trim(pi.class_cd))=5 then concat('0',pi.class_cd) else pi.class_cd  end) as int64),
ep.CLASS_CD as epsilon_class_cd
--sum(case when cast(concat(pi.department_cd,pi.class_cd)	as  int64)				=ep.CLASS_CD      then 1 else 0 end) as match_class_count
--,sum(case when cast(concat(pi.department_cd,pi.class_cd)	as int64)				=ep.CLASS_CD      then 0 else 1 end) as mismatch_class_count

-- sum(case when cast(concat(pi.department_cd,case when length(trim(pi.class_cd))=5 then concat('0',pi.class_cd) else pi.class_cd  end) as int64)=ep.CLASS_CD then 1 else 0 end) as match_class_count
-- ,sum(case when cast(concat(pi.department_cd,case when length(trim(pi.class_cd))=5 then concat('0',pi.class_cd) else pi.class_cd  end) as int64)=ep.CLASS_CD then 0 else 1 end) as mismatch_class_count

from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
where  length(pi.class_cd)!=6
and cast(concat(pi.department_cd,case when length(trim(pi.class_cd))=5 then concat('0',pi.class_cd) else pi.class_cd  end) as int64)!=ep.CLASS_CD




select
pi.department_cd,
--pi.class_cd,
case when  length(trim(pi.class_cd))=5 then concat('0',pi.class_cd) else pi.class_cd  end,
--length(trim(pi.class_cd))=6 then pi.class_cd                     
                      ep.CLASS_CD as epsilon_class_cd
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
where cast(concat(pi.department_cd,pi.class_cd)	as  int64)				!= ep.CLASS_CD
--and length(pi.class_cd)=2

-- select length(class_cd),count(*)
-- from p-ascn-da-aadp-001.analytic_mart.pre_item
-- group by 1;



select pi.department_cd as mart_dept
      ,pi.class_cd as mart_class
      ,pi.class_des as mart_desc
      ,concat(cast(pi.department_cd as string)
             ,lpad(pi.class_cd,6,'0')
            )                           as class_contact
             ,ep.DEPT_CD
             ,ep.CLASS_CD                           as epsilon_class_cd
             ,ep.CLASS_DESC
          from p-asna-analytics-002.work.csm_loft_product ep
         inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
            on ep.sku=pi.sku_id
         where concat(cast(pi.department_cd as string)
                      ,lpad(pi.class_cd,6,'0')
                     )   != ep.CLASS_CD
         and length(pi.class_cd)=5
limit 1000

select length(pi.class_cd)
  ,count(*)
from p-ascn-da-aadp-001.analytic_mart.pre_item pi
join p-asna-analytics-002.work.csm_loft_product ep
    on ep.sku=pi.sku_id
group by length(pi.class_cd)


-----------------------------------------------------------------------------------------------
--simple concat
{code}
select
sum(case when concat(pi.department_cd,pi.class_cd)				=ep.CLASS_CD      then 1 else 0 end) as match_class_count
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
{code}
match_class_count 
2924568   		

-- appending with 0 at start of class_cd
{code}
select
sum(case when concat(pi.department_cd,'0',pi.class_cd) =ep.CLASS_CD then 1 else 0 end) as match_class_count
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
{code}
match_class_count 
302282   		

-- removing the first character of class_cd
{code}
select
sum(case when concat(pi.department_cd,substr(pi.class_cd,2)) =ep.CLASS_CD then 1 else 0 end) as match_class_count
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
{code}
match_class_count 
108784   				

-- removing the third character of class_cd
{code}
select
sum(case when concat(pi.department_cd,substr(pi.class_cd,1,2),substr(pi.class_cd,4)) =ep.CLASS_CD then 1 else 0 end) as match_class_count
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
{code}
match_class_count
5768   			

--class_cd with 0 and null values
{code}
select
sum(case when coalesce(cast(pi.class_cd as int64),0) = coalesce(cast(ep.CLASS_CD as int64),0) then 1 else 0 end) as match_class_count
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
{code}
match_class_count
41778   			



select
pi.sku_id as mart_sku
,pi.department_cd as mart_dept
,pi.class_cd as mart_class
,pi.class_des as mart_desc
,ep.sku as epsilon_sku
,ep.DEPT_CD   as epsilon_dept_cd
,ep.CLASS_CD  as epsilon_class_cd
,ep.CLASS_DESC as epsilon_desc
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
where 
((concat(pi.department_cd,pi.class_cd) != ep.CLASS_CD) and (concat(pi.department_cd,'0',pi.class_cd) !=ep.CLASS_CD) and (concat(pi.department_cd,substr(pi.class_cd,2)) !=ep.CLASS_CD)   and (concat(pi.department_cd,substr(pi.class_cd,1,2),substr(pi.class_cd,4)) !=ep.CLASS_CD) and coalesce(cast(pi.class_cd as int64),0) != coalesce(cast(ep.CLASS_CD as int64),0))


--37 actual mismatch


select
pi.department_cd as mart_dept
,pi.class_cd as mart_class
,pi.class_des as mart_desc
,ep.DEPT_CD   as epsilon_dept_cd
,ep.CLASS_CD  as epsilon_class_cd
,ep.CLASS_DESC as epsilon_desc
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
where 
((concat(pi.department_cd,pi.class_cd) = ep.CLASS_CD) or (concat(pi.department_cd,'0',pi.class_cd) =ep.CLASS_CD) or (concat(pi.department_cd,substr(pi.class_cd,2)) =ep.CLASS_CD)   or (concat(pi.department_cd,substr(pi.class_cd,1,2),substr(pi.class_cd,4)) =ep.CLASS_CD) or coalesce(cast(pi.class_cd as int64),0) = coalesce(cast(ep.CLASS_CD as int64),0))


--3377479 match 



select
sum(case when coalesce(pi.department_cd,0)	=coalesce(cast(ep.DEPT_CD as int64),0)  then 1 else 0 end) as match_dept_count
,sum(case when coalesce(pi.department_cd,0)	=coalesce(cast(ep.DEPT_CD as int64),0)  then 0 else 1 end) as mismatch_dept_count
,sum(case when pi.style_id					=cast(ep.STYLE_NBR as int64)    then 1 else 0 end) as match_style_count
,sum(case when pi.style_id					=cast(ep.STYLE_NBR as int64)     then 0 else 1 end) as mismatch_style_count
,sum(case when pi.color_cd					=cast(ep.COLOR_CD as int64)      then 1 else 0 end) as match_color_count
,sum(case when pi.color_cd					=cast(ep.COLOR_CD as int64)     then 0 else 1 end) as mismatch_color_count
,sum(case when pi.size_cd					=cast(ep.SIZE_CD  as int64)    then 1 else 0 end) as match_size_count
,sum(case when pi.size_cd					=cast(ep.SIZE_CD  as int64)       then 0 else 1 end) as mismatch_size_count
,sum(case when coalesce(pi.department_des,'')					=coalesce(ep.DEPT_DESC,'')    then 1 else 0 end) as match_dept_des
,sum(case when coalesce(pi.department_des,'')					=coalesce(ep.DEPT_DESC,'')     then 0 else 1 end) as mismatch_dept_des
,sum(case when coalesce(pi.class_des,'')					=coalesce(ep.CLASS_DESC,'')    then 1 else 0 end) as match_class_des
,sum(case when coalesce(pi.class_des,'')					=coalesce(ep.CLASS_DESC,'')     then 0 else 1 end) as mismatch_class_des
,sum(case when coalesce(pi.style_des,'')					=coalesce(ep.STYLE_DESC,'')    then 1 else 0 end) as match_style_des
,sum(case when coalesce(pi.style_des,'')					=coalesce(ep.STYLE_DESC,'')     then 0 else 1 end) as mismatch_style_des
,sum(case when coalesce(pi.color_des,'')					=coalesce(ep.COLOR_DESC,'')    then 1 else 0 end) as match_color_des
,sum(case when coalesce(pi.color_des,'')					=coalesce(ep.COLOR_DESC,'')     then 0 else 1 end) as mismatch_color_des
,sum(case when coalesce(pi.size_des,'')					=coalesce(ep.SIZE_DESC,'')    then 1 else 0 end) as match_size_des
,sum(case when coalesce(pi.size_des,'')					=coalesce(ep.SIZE_DESC,'')     then 0 else 1 end) as mismatch_size_des
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
where 
((concat(pi.department_cd,pi.class_cd) = ep.CLASS_CD) or (concat(pi.department_cd,'0',pi.class_cd) =ep.CLASS_CD) or (concat(pi.department_cd,substr(pi.class_cd,2)) =ep.CLASS_CD)   or (concat(pi.department_cd,substr(pi.class_cd,1,2),substr(pi.class_cd,4)) =ep.CLASS_CD) or coalesce(cast(pi.class_cd as int64),0) = coalesce(cast(ep.CLASS_CD as int64),0))


--------------------------------------------------------------------------------------------------------

select
sum(case when coalesce(pi.department_cd,0)	=coalesce(cast(ep.DEPT_CD as int64),0)  then 1 else 0 end) as match_dept_count
,sum(case when coalesce(pi.department_cd,0)	=coalesce(cast(ep.DEPT_CD as int64),0)  then 0 else 1 end) as mismatch_dept_count
,sum(case when pi.style_id					=cast(ep.STYLE_NBR as int64)    then 1 else 0 end) as match_style_count
,sum(case when pi.style_id					=cast(ep.STYLE_NBR as int64)     then 0 else 1 end) as mismatch_style_count
,sum(case when pi.color_cd					=cast(ep.COLOR_CD as int64)      then 1 else 0 end) as match_color_count
,sum(case when pi.color_cd					=cast(ep.COLOR_CD as int64)     then 0 else 1 end) as mismatch_color_count
,sum(case when pi.size_cd					=cast(ep.SIZE_CD  as int64)    then 1 else 0 end) as match_size_count
,sum(case when pi.size_cd					=cast(ep.SIZE_CD  as int64)       then 0 else 1 end) as mismatch_size_count
,sum(case when coalesce(pi.department_des,'')					=coalesce(ep.DEPT_DESC,'')    then 1 else 0 end) as match_dept_des
,sum(case when coalesce(pi.department_des,'')					=coalesce(ep.DEPT_DESC,'')     then 0 else 1 end) as mismatch_dept_des
,sum(case when coalesce(pi.class_des,'')					=coalesce(ep.CLASS_DESC,'')    then 1 else 0 end) as match_class_des
,sum(case when coalesce(pi.class_des,'')					=coalesce(ep.CLASS_DESC,'')     then 0 else 1 end) as mismatch_class_des
,sum(case when coalesce(pi.style_des,'')					=coalesce(ep.STYLE_DESC,'')    then 1 else 0 end) as match_style_des
,sum(case when coalesce(pi.style_des,'')					=coalesce(ep.STYLE_DESC,'')     then 0 else 1 end) as mismatch_style_des
,sum(case when coalesce(pi.color_des,'')					=coalesce(ep.COLOR_DESC,'')    then 1 else 0 end) as match_color_des
,sum(case when coalesce(pi.color_des,'')					=coalesce(ep.COLOR_DESC,'')     then 0 else 1 end) as mismatch_color_des
,sum(case when coalesce(pi.size_des,'')					=coalesce(ep.SIZE_DESC,'')    then 1 else 0 end) as match_size_des
,sum(case when coalesce(pi.size_des,'')					=coalesce(ep.SIZE_DESC,'')     then 0 else 1 end) as mismatch_size_des
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
where 
((concat(pi.department_cd,pi.class_cd) = ep.CLASS_CD) or (concat(pi.department_cd,'0',pi.class_cd) =ep.CLASS_CD) or (concat(pi.department_cd,substr(pi.class_cd,2)) =ep.CLASS_CD)   or (concat(pi.department_cd,substr(pi.class_cd,1,2),substr(pi.class_cd,4)) =ep.CLASS_CD) or coalesce(cast(pi.class_cd as int64),0) = coalesce(cast(ep.CLASS_CD as int64),0))

-----------------------------------------------------------------------------------------

select
count(distinct td.transaction_key),max(td.transaction_dt), min(td.transaction_dt)
from p-ascn-da-aadp-001.analytic_mart.pre_transaction_item td
inner join p-ascn-da-aadp-001.analytic_mart.pre_item i
on td.item_key=i.item_key
where coalesce(i.sku_id,'') in
( '30308305'
,'30306608'
,'30308701'
,'30305205'
,'30304703'
,'30309005'
,'30308909'
,'30307803'
,'30308602'
,'30304901'
,'30309104'
,'30307902'
,'30305304'
,'30305007'
,'30309203'
,'30304505'
,'30306707'
,'30305502'
,'30308404'
,'30307209'
,'30304604'
,'30307704'
,'30308206'
,'30307605'
,'30308107'
,'30305403'
,'30306103'
,'30304802'
,'0'
,'30306301'
,'30304406'
,'30307308'
,'30305106'
,'30308503'
,'30308008'
,'30306509'
,'30304307')

--3377435   3377516

--3377479 

select
sum(case when coalesce(pi.department_cd,0)	=coalesce(cast(ep.DEPT_CD as int64),0)  then 1 else 0 end) as match_dept_count
,sum(case when coalesce(pi.department_cd,0)	=coalesce(cast(ep.DEPT_CD as int64),0)  then 0 else 1 end) as mismatch_dept_count
,sum(case when pi.style_id					=cast(ep.STYLE_NBR as int64)    then 1 else 0 end) as match_style_count
,sum(case when pi.style_id					=cast(ep.STYLE_NBR as int64)     then 0 else 1 end) as mismatch_style_count
,sum(case when pi.color_cd					=cast(ep.COLOR_CD as int64)      then 1 else 0 end) as match_color_count
,sum(case when pi.color_cd					=cast(ep.COLOR_CD as int64)     then 0 else 1 end) as mismatch_color_count
,sum(case when pi.size_cd					=cast(ep.SIZE_CD  as int64)    then 1 else 0 end) as match_size_count
,sum(case when pi.size_cd					=cast(ep.SIZE_CD  as int64)       then 0 else 1 end) as mismatch_size_count

,sum(case when coalesce(upper(trim(pi.department_des)),'')					=coalesce(upper(trim(ep.DEPT_DESC)),'')    then 1 else 0 end) as match_dept_des
,sum(case when coalesce(upper(trim(pi.department_des)),'')				=coalesce(upper(trim(ep.DEPT_DESC)),'')     then 0 else 1 end) as mismatch_dept_des
,sum(case when coalesce(upper(trim(pi.class_des)),'')					=coalesce(upper(trim(ep.CLASS_DESC)),'')    then 1 else 0 end) as match_class_des
,sum(case when coalesce(upper(trim(pi.class_des)),'')					=coalesce(upper(trim(ep.CLASS_DESC)),'')     then 0 else 1 end) as mismatch_class_des
,sum(case when coalesce(upper(trim(pi.style_des)),'')					=coalesce(upper(trim(ep.STYLE_DESC)),'')    then 1 else 0 end) as match_style_des
,sum(case when coalesce(upper(trim(pi.style_des)),'')					=coalesce(upper(trim(ep.STYLE_DESC)),'')     then 0 else 1 end) as mismatch_style_des
,sum(case when coalesce(upper(trim(pi.color_des)),'')					=coalesce(upper(trim(ep.COLOR_DESC)),'')    then 1 else 0 end) as match_color_des
,sum(case when coalesce(upper(trim(pi.color_des)),'')					=coalesce(upper(trim(ep.COLOR_DESC)),'')     then 0 else 1 end) as mismatch_color_des
,sum(case when coalesce(upper(trim(pi.size_des)),'')					=coalesce(upper(trim(ep.SIZE_DESC)),'')    then 1 else 0 end) as match_size_des
,sum(case when coalesce(upper(trim(pi.size_des)),'')					=coalesce(upper(trim(ep.SIZE_DESC)),'')     then 0 else 1 end) as mismatch_size_des

from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
where 
((concat(pi.department_cd,pi.class_cd) = ep.CLASS_CD) or (concat(pi.department_cd,'0',pi.class_cd) =ep.CLASS_CD) or (concat(pi.department_cd,substr(pi.class_cd,2)) =ep.CLASS_CD)   or (concat(pi.department_cd,substr(pi.class_cd,1,2),substr(pi.class_cd,4)) =ep.CLASS_CD) or coalesce(cast(pi.class_cd as int64),0) = coalesce(cast(ep.CLASS_CD as int64),0))



-----------------------------------------------------

select
 sum(case when coalesce(pi.department_cd,0)	=coalesce(cast(ep.DEPT_CD as int64),0)  then 1 else 0 end) as match_dept_count
,sum(case when coalesce(pi.department_cd,0)	=coalesce(cast(ep.DEPT_CD as int64),0)  then 0 else 1 end) as mismatch_dept_count
,sum(case when pi.style_id					=cast(ep.STYLE_NBR as int64)    then 1 else 0 end) as match_style_count
,sum(case when pi.style_id					=cast(ep.STYLE_NBR as int64)     then 0 else 1 end) as mismatch_style_count
,sum(case when pi.color_cd					=cast(ep.COLOR_CD as int64)      then 1 else 0 end) as match_color_count
,sum(case when pi.color_cd					=cast(ep.COLOR_CD as int64)     then 0 else 1 end) as mismatch_color_count
,sum(case when pi.size_cd					=cast(ep.SIZE_CD  as int64)    then 1 else 0 end) as match_size_count
,sum(case when pi.size_cd					=cast(ep.SIZE_CD  as int64)       then 0 else 1 end) as mismatch_size_count

,sum(case when coalesce(array_to_string(split(upper(trim(pi.department_des)),' '),''),'UNDEFINED')					= coalesce(array_to_string(split(upper(trim(ep.DEPT_DESC)),' '),''),'UNDEFINED')    then 1 else 0 end) as match_dept_des

,sum(case when coalesce(array_to_string(split(upper(trim(pi.department_des)),' '),''),'UNDEFINED')					= coalesce(array_to_string(split(upper(trim(ep.DEPT_DESC)),' '),''),'UNDEFINED')     then 0 else 1 end) as mismatch_dept_des

,sum(case when coalesce(array_to_string(split(upper(trim(pi.class_des)),' '),''),'UNDEFINED')					= coalesce(array_to_string(split(upper(trim(ep.CLASS_DESC)),' '),''),'UNDEFINED')    then 1 else 0 end) as match_class_des

,sum(case when coalesce(array_to_string(split(upper(trim(pi.class_des)),' '),''),'UNDEFINED')					= coalesce(array_to_string(split(upper(trim(ep.CLASS_DESC)),' '),''),'UNDEFINED')     then 0 else 1 end) as mismatch_class_des

,sum(case when coalesce(array_to_string(split(upper(trim(pi.style_des)),' '),''),'UNDEFINED')					= coalesce(array_to_string(split(upper(trim(ep.STYLE_DESC)),' '),''),'UNDEFINED')    then 1 else 0 end) as match_style_des

,sum(case when coalesce(array_to_string(split(upper(trim(pi.style_des)),' '),''),'UNDEFINED')					= coalesce(array_to_string(split(upper(trim(ep.STYLE_DESC)),' '),''),'UNDEFINED')     then 0 else 1 end) as mismatch_style_des

,sum(case when coalesce(array_to_string(split(upper(trim(pi.color_des)),' '),''),'UNDEFINED')					= coalesce(array_to_string(split(upper(trim(ep.COLOR_DESC)),' '),''),'UNDEFINED')    then 1 else 0 end) as match_color_des

,sum(case when coalesce(array_to_string(split(upper(trim(pi.color_des)),' '),''),'UNDEFINED')					= coalesce(array_to_string(split(upper(trim(ep.COLOR_DESC)),' '),''),'UNDEFINED')     then 0 else 1 end) as mismatch_color_des

,sum(case when coalesce(array_to_string(split(upper(trim(pi.size_des)),' '),''),'UNDEFINED')					= coalesce(array_to_string(split(upper(trim(ep.SIZE_DESC)),' '),''),'UNDEFINED')    then 1 else 0 end) as match_size_des

,sum(case when coalesce(array_to_string(split(upper(trim(pi.size_des)),' '),''),'UNDEFINED')					= coalesce(array_to_string(split(upper(trim(ep.SIZE_DESC)),' '),''),'UNDEFINED')     then 0 else 1 end) as mismatch_size_des

from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
where 
((concat(pi.department_cd,pi.class_cd) = ep.CLASS_CD) 
or (concat(pi.department_cd,'0',pi.class_cd) =ep.CLASS_CD) or (concat(pi.department_cd,substr(pi.class_cd,2)) =ep.CLASS_CD)   or (concat(pi.department_cd,substr(pi.class_cd,1,2),substr(pi.class_cd,4)) =ep.CLASS_CD) or coalesce(cast(pi.class_cd as int64),0) = coalesce(cast(ep.CLASS_CD as int64),0))
;


select
pi.style_des
,pi.color_des
,ep.STYLE_DESC
,ep.COLOR_DESC
from p-asna-analytics-002.work.csm_loft_product ep
inner join p-ascn-da-aadp-001.analytic_mart.pre_item pi
on ep.sku=pi.sku_id
where 
((concat(pi.department_cd,pi.class_cd) = ep.CLASS_CD) or (concat(pi.department_cd,'0',pi.class_cd) =ep.CLASS_CD) or (concat(pi.department_cd,substr(pi.class_cd,2)) =ep.CLASS_CD)   or (concat(pi.department_cd,substr(pi.class_cd,1,2),substr(pi.class_cd,4)) =ep.CLASS_CD) or coalesce(cast(pi.class_cd as int64),0) = coalesce(cast(ep.CLASS_CD as int64),0))
--and coalesce(upper(trim(pi.department_des)),'')					!=coalesce(upper(trim(ep.DEPT_DESC)),'')
--and coalesce(array_to_string(split(upper(trim(pi.class_des)),' '),''),'UNDEFINED')					!= coalesce(array_to_string(split(upper(trim(ep.CLASS_DESC)),' '),''),'UNDEFINED') 
--and coalesce(array_to_string(split(upper(trim(pi.style_des)),' '),''),'UNDEFINED')					!= coalesce(array_to_string(split(upper(trim(ep.STYLE_DESC)),' '),''),'UNDEFINED')
and coalesce(array_to_string(split(upper(trim(pi.color_des)),' '),''),'UNDEFINED')					!= coalesce(array_to_string(split(upper(trim(ep.COLOR_DESC)),' '),''),'UNDEFINED')





-- select
-- * from
-- p-asna-analytics-002.work.csm_loft_product ep
-- where ep.DEPT_DESC='UNDEFINED' or
-- ep.CLASS_DESC ='UNDEFINED'or
-- ep.STYLE_DESC ='UNDEFINED' or
-- ep.COLOR_DESC ='UNDEFINED' or
-- ep.SIZE_DESC  ='UNDEFINED'