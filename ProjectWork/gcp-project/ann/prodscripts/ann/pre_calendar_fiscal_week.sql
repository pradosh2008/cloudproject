create or replace table analytic_mart.pre_calendar_fiscal_week
as
select
distinct
d.fiscal_week_id
,sw.day_dt as fiscal_week_start_dt
,ew.day_dt as fiscal_week_end_dt
,fiscal_week_of_year as fiscal_week_of_yr
,fiscal_month_id
,fiscal_month_of_year as fiscal_month_of_yr
,fiscal_month_nm
,fiscal_quarter_id
,fiscal_quarter_nbr
,fiscal_year_id
from 
edl_stage.pre_ann_calendar d
left outer join
(select fiscal_week_id
        ,day_dt
from edl_stage.pre_ann_calendar
    where fiscal_day_of_week=1) as sw
on d.fiscal_week_id=sw.fiscal_week_id
left outer join 
(select fiscal_week_id
        ,day_dt
from edl_stage.pre_ann_calendar
    where fiscal_day_of_week=7) as ew
on d.fiscal_week_id=ew.fiscal_week_id