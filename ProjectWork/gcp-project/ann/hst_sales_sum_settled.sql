create or replace table work.hist_sales_sum_settled as 
select  
 Store_ID
 ,Week_ID
 ,Style_ID
 ,Color_ID
 ,sum(Net_Sales_Unit_cnt)Net_Sales_Unit_cnt
 ,sum(Net_Sales_Amt)Net_Sales_Amt
 ,sum(Net_Sales_COGS_amt)Net_Sales_COGS_amt
 ,sum(Return_Sales_Unit_cnt)Return_Sales_Unit_cnt
 ,sum(Return_Sales_Dollars_amt)Return_Sales_Dollars_amt
 ,sum(Return_Sales_COGS_amt)Return_Sales_COGS_amt
 ,sum(Promotion_Net_Sales_Unit_cnt)Promotion_Net_Sales_Unit_cnt
 ,sum(Promotion_Net_Sales_Dollars_amt)Promotion_Net_Sales_Dollars_amt
 ,sum(Promotion_Net_Sales_COGS_amt)Promotion_Net_Sales_COGS_amt
 ,sum(Markdown_Net_Sales_Unit_cnt)Markdown_Net_Sales_Unit_cnt
 ,sum(Markdown_Net_Sales_Dollars_amt)Markdown_Net_Sales_Dollars_amt
 ,round(sum(Markdown_Net_Sales_COGS_amt),2) Markdown_Net_Sales_COGS_amt
from
	(select 
		store_nbr Store_ID
		,fiscal_week_id Week_ID
		,cast(pr.style_nbr as string) Style_ID
		,pr.color_cd Color_ID
		,sku
		,extract (date from txn_dt) as Day_dt
		,case when item_sold_qty>0  Then item_sold_qty else -item_ret_qty end as  Net_Sales_Unit_cnt
		,case when item_sold_qty>0 Then item_net_amt else -item_ret_amt end as  Net_Sales_Amt
		,case when item_sold_qty>0 Then round(item_cogs,2)*item_sold_qty else -round(item_cogs,2)*item_ret_qty end as  Net_Sales_COGS_amt
		,case when item_ret_qty>0 Then item_ret_qty else -item_ret_qty end as  Return_Sales_Unit_cnt
		,case when item_ret_qty>0 Then item_ret_amt else 0 end as  Return_Sales_Dollars_amt
		,case when item_ret_qty>0 Then round(item_cogs,2)*item_ret_qty else 0 end as  Return_Sales_COGS_amt
		,case when t.FULL_PRICE_FLG = 'N' and item_sold_qty > 0  THEN item_sold_qty 
				when t.FULL_PRICE_FLG = 'N' and item_ret_qty > 0  THEN -item_ret_qty 
				else 0 
		end  as Markdown_Net_Sales_Unit_cnt
		,case when t.FULL_PRICE_FLG = 'N' and item_sold_qty > 0  THEN item_net_amt
				when t.FULL_PRICE_FLG = 'N' and item_ret_qty > 0  THEN -item_ret_amt 
				else 0 
		end  as Markdown_Net_Sales_Dollars_amt      
		,case when t.FULL_PRICE_FLG = 'N'  and item_sold_qty > 0 THEN round(item_cogs,2)
				when t.FULL_PRICE_FLG = 'N' and item_ret_qty > 0  THEN -round(item_cogs,2) 
				else 0 
		end  as Markdown_Net_Sales_COGS_amt            
		,CASE when item_ret_qty>0 And full_price_flg='F' then -item_ret_qty
				when item_sold_qty>0 And full_price_flg='F' then item_sold_qty
				else 0 
		end as Promotion_Net_Sales_Unit_cnt
		,CASE when item_ret_qty>0 And full_price_flg='F' then -round(item_ret_amt,2)
				when item_sold_qty>0 And full_price_flg='F' then round(item_net_amt,2)
				else 0 
		end as Promotion_Net_Sales_Dollars_amt      
		,CASE when item_ret_qty>0 And full_price_flg='F' then -round(item_cogs,2)
				when item_sold_qty>0 And full_price_flg='F' then round(item_cogs,2)
				else 0 
		end as Promotion_Net_Sales_COGS_amt
FROM work.ann_hst_f_txn_item_2016 t
left join edl_landing.ann_calendar cal
  on CAST (day_dt AS date) = EXTRACT(date FROM txn_dt) 
left join edl_landing.lu_product_vw pr
  on pr.product_id = t.product_id 
    )
  group by  Store_ID,Week_ID,Style_ID,Color_ID;