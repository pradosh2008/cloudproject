select count(*) from 
(
select
dem.Store_ID as Store_ID
,dem.Week_ID as Week_ID
,dem.Style_ID as Style_ID
,dem.Color_ID as Color_ID
,dem.Net_Sales_Unit_cnt
,hst.Net_Sales_Unit_cnt     as hst_Net_Sales_Unit_cnt
,case when hst.Net_Sales_Unit_cnt=0 then abs(dem.Net_Sales_Unit_cnt) else abs(dem.Net_Sales_Unit_cnt-hst.Net_Sales_Unit_cnt) end as diff_Net_Sales_Unit_cnt
,dem.Net_Sales_Amt
,hst.Net_Sales_Amt as hst_Net_Sales_Amt
,case when hst.Net_Sales_Amt=0 then abs(dem.Net_Sales_Amt) else abs(dem.Net_Sales_Amt-hst.Net_Sales_Amt)/hst.Net_Sales_Amt end as perc_diff_Net_Sales_Amt
from
demand_forecast.sales_sum_placed as dem
inner join
work.hist_sales_sum_settled as hst
on
dem.Store_ID=hst.Store_ID
and dem.Week_ID =hst.Week_ID 
and cast(dem.Style_ID as int64)=cast(hst.Style_ID as int64)
and cast(dem.Color_ID AS INT64)=hst.Color_ID
where dem.Week_ID>201830 and dem.Week_ID<201916
and dem.Store_ID in (611,612,613,616,617)
)
where diff_Net_Sales_Unit_cnt=0

--158338

select count(*) from 
(
select
dem.Store_ID as Store_ID
,dem.Week_ID as Week_ID
,dem.Style_ID as Style_ID
,dem.Color_ID as Color_ID
,dem.Net_Sales_Unit_cnt
,hst.Net_Sales_Unit_cnt     as hst_Net_Sales_Unit_cnt
,case when hst.Net_Sales_Unit_cnt=0 then abs(dem.Net_Sales_Unit_cnt) else abs(dem.Net_Sales_Unit_cnt-hst.Net_Sales_Unit_cnt) end as diff_Net_Sales_Unit_cnt
,dem.Net_Sales_Amt
,hst.Net_Sales_Amt as hst_Net_Sales_Amt
,case when dem.Net_Sales_Amt=0 then abs(-hst.Net_Sales_Amt)*100 else (abs(dem.Net_Sales_Amt-hst.Net_Sales_Amt)/dem.Net_Sales_Amt)*100 end as perc_diff_Net_Sales_Amt
from
demand_forecast.sales_sum_settled as dem
inner join
work.hist_sales_sum_settled as hst
on
dem.Store_ID=hst.Store_ID
and dem.Week_ID =hst.Week_ID 
and cast(dem.Style_ID as int64)=cast(hst.Style_ID as int64)
and cast(dem.Color_ID AS INT64)=hst.Color_ID
where dem.Week_ID>201830 and dem.Week_ID<201916
and dem.Store_ID in (611,612,613,616,617)
)
where diff_Net_Sales_Unit_cnt=0

--71015



-- history POS data with sales_sum_settled 
select count(*) from 
(
select
dem.Store_ID as Store_ID
,dem.Week_ID as Week_ID
,dem.Style_ID as Style_ID
,dem.Color_ID as Color_ID
,dem.Net_Sales_Unit_cnt
,hst.Net_Sales_Unit_cnt     as hst_Net_Sales_Unit_cnt
,case when hst.Net_Sales_Unit_cnt=0 then abs(dem.Net_Sales_Unit_cnt) else abs(dem.Net_Sales_Unit_cnt-hst.Net_Sales_Unit_cnt) end as diff_Net_Sales_Unit_cnt
,dem.Net_Sales_Amt
,hst.Net_Sales_Amt as hst_Net_Sales_Amt
,case when dem.Net_Sales_Amt=0 then abs(-hst.Net_Sales_Amt)*100 else (abs(dem.Net_Sales_Amt-hst.Net_Sales_Amt)/dem.Net_Sales_Amt)*100 end as perc_diff_Net_Sales_Amt
from
demand_forecast.sales_sum_settled as dem
inner join
work.hist_sales_sum_settled as hst
on
dem.Store_ID=hst.Store_ID
and dem.Week_ID =hst.Week_ID 
and cast(dem.Style_ID as int64)=cast(hst.Style_ID as int64)
and cast(dem.Color_ID AS INT64)=hst.Color_ID
where dem.Week_ID>201830 and dem.Week_ID<201916
and dem.Store_ID not in (611,612,613,616,617)
)
where diff_Net_Sales_Unit_cnt=0

--24760710


For ecom data : weekid Range : Week_ID>201830 and Week_ID<201916
---- history ecom data against sales_sum_settled , total 460424 combination , where diff_Net_Sales_Unit_cnt=0 --71015
---- history ecom data against sales_sum_placed , total 462470 combination , where diff_Net_Sales_Unit_cnt=0 158338


For History data : weekid Range : Week_ID>201830 and Week_ID<201916
-- history POS data against sales_sum_settled   , Total = 25204806 combination , where diff_Net_Sales_Unit_cnt=0 24760710 
 