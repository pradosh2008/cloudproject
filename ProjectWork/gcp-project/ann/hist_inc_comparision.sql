-----------------------------------------------------------store,week,style,color level between historical and incremental comparison of net and return measures

select
inc.Store_ID as Store_ID
,inc.Week_ID as Week_ID
,inc.Style_ID as Style_ID
,inc.Color_ID as Color_ID
,inc.Net_Sales_Unit_cnt     as inc_Net_Sales_Unit_cnt
,inc.Net_Sales_Amt as inc_Net_Sales_Amt
,hst.Net_Sales_Unit_cnt     as hst_Net_Sales_Unit_cnt
,hst.Net_Sales_Amt as hst_Net_Sales_Amt
,inc.Net_Sales_Unit_cnt-hst.Net_Sales_Unit_cnt as diff_net_cnt
,inc.Net_Sales_Amt-hst.Net_Sales_Amt as diff_net_amt
,inc.Return_Sales_Unit_cnt as inc_Return_Sales_Unit_cnt
,inc.Return_Sales_Dollars_amt as inc_Return_Sales_Dollars_amt
,hst.Return_Sales_Unit_cnt as hst_Return_Sales_Unit_cnt
,hst.Return_Sales_Dollars_amt as hst_Return_Sales_Dollars_amt
,inc.Return_Sales_Unit_cnt-hst.Return_Sales_Unit_cnt as diff_return_net_cnt
,inc.Return_Sales_Dollars_amt-hst.Return_Sales_Dollars_amt as diff_return_net_amt
from
analytic_mart.pre_sales_sum_settled_pos as inc
inner join
work.hist_sales_sum_settled_pos as hst
	on
inc.Store_ID=hst.Store_ID
	and inc.Week_ID =hst.Week_ID 
	and cast(inc.Style_ID AS INT64)=cast(hst.Style_ID as int64)
	and CAST(inc.Color_ID AS INT64)=hst.Color_ID
where	inc.Week_ID>=201830 
	and inc.Week_ID<=201916


------------------------------------------------------------------week_level between historical and incremental comparison of net and return measures

select
a.week_id
,inc_Net_Sales_Unit_cnt
,inc_Net_Sales_Amt
,hst_Net_Sales_Unit_cnt
,hst_Net_Sales_Amt
,inc_Net_Sales_Unit_cnt-hst_Net_Sales_Unit_cnt as diff_net_cnt
,inc_Net_Sales_Amt-hst_Net_Sales_Amt as diff_net_amt
,inc_Return_Sales_Unit_cnt
,hst_Return_Sales_Unit_cnt
,inc_Return_Sales_Dollars_amt
,hst_Return_Sales_Dollars_amt
,inc_Return_Sales_Unit_cnt-hst_Return_Sales_Unit_cnt as diff_hst_net_cnt
,inc_Return_Sales_Dollars_amt-hst_Return_Sales_Dollars_amt as diff_hst_net_amt
from	(
	select
	inc.Week_ID
	,sum(inc.Net_Sales_Unit_cnt)   as inc_Net_Sales_Unit_cnt
	,sum(inc.Net_Sales_Amt) as inc_Net_Sales_Amt
	,sum(inc.Return_Sales_Unit_cnt) as inc_Return_Sales_Unit_cnt
	,sum(inc.Return_Sales_Dollars_amt) as inc_Return_Sales_Dollars_amt
	from	analytic_mart.pre_sales_sum_settled_pos as inc
	group by inc.Week_ID
) as a 
inner join
(
	select
	hst.week_id
	,sum(hst.Net_Sales_Unit_cnt)     as hst_Net_Sales_Unit_cnt
	,sum(hst.Net_Sales_Amt) as hst_Net_Sales_Amt
	,sum(hst.Return_Sales_Unit_cnt) as hst_Return_Sales_Unit_cnt
	,sum(hst.Return_Sales_Dollars_amt) as hst_Return_Sales_Dollars_amt
	 from	work.hist_sales_sum_settled_pos as hst
	group by hst.week_id
) as b
	on
a.Week_ID =b.Week_ID 


---------------------------------------------------------------------
select
sum(
        case 
            when diff_net_cnt=0 then 1 
            else 0 
        end) as matching_hst_Net_Sales_Unit_cnt
,sum(
        case 
            when diff_net_cnt=0 then 0 
            else 1 
        end) as not_matching_hst_Net_Sales_Unit_cnt          
from    
(
select
inc.Store_ID as Store_ID
,inc.Week_ID as Week_ID
,inc.Style_ID as Style_ID
,inc.Color_ID as Color_ID
,inc.Net_Sales_Unit_cnt     as inc_Net_Sales_Unit_cnt
,inc.Net_Sales_Amt as inc_Net_Sales_Amt
,hst.Net_Sales_Unit_cnt     as hst_Net_Sales_Unit_cnt
,hst.Net_Sales_Amt as hst_Net_Sales_Amt
,inc.Net_Sales_Unit_cnt-hst.Net_Sales_Unit_cnt as diff_net_cnt
,inc.Net_Sales_Amt-hst.Net_Sales_Amt as diff_net_amt
,inc.Return_Sales_Unit_cnt as inc_Return_Sales_Unit_cnt
,inc.Return_Sales_Dollars_amt as inc_Return_Sales_Dollars_amt
,hst.Return_Sales_Unit_cnt as hst_Return_Sales_Unit_cnt
,hst.Return_Sales_Dollars_amt as hst_Return_Sales_Dollars_amt
,inc.Return_Sales_Unit_cnt-hst.Return_Sales_Unit_cnt as diff_hst_net_cnt
,inc.Return_Sales_Dollars_amt-hst.Return_Sales_Dollars_amt as diff_hst_net_amt
from
analytic_mart.pre_sales_sum_settled_pos as inc
inner join
work.hist_sales_sum_settled_pos as hst
	on
inc.Store_ID=hst.Store_ID
	and inc.Week_ID =hst.Week_ID 
	and cast(inc.Style_ID AS INT64)=cast(hst.Style_ID as int64)
	and CAST(inc.Color_ID AS INT64)=hst.Color_ID
where	inc.Week_ID>=201830 
	and inc.Week_ID<=201916 )