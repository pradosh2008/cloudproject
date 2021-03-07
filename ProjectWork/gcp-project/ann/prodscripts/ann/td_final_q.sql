select 
 transaction_dt
 -- ,customer_nbr
 --,count( distinct customer_nbr)cnt_cust_td
, sum(CNT_CUSTOMER_NBR) AS cnt_customer_nbr
, count(distinct txn_key) cnt_txn_td

, sum(case when SALE_TYPE_IND = 'SALE' then RETAIL_SALES_AMT else 0 end) as sum_sales_td

, sum(case when SALE_TYPE_IND = 'SALE' then RETAIL_QUANTITY_CNT else 0 end) as sum_qty_td

, sum(RETURN_SALES_AMT) as RETURN_SALES_AMT_td

, sum(RETURN_QUANTITY_CNT) as RETURN_QUANTITY_CNT_TD

,sum(case when SALE_TYPE_IND = 'SALE' then net_unit_cost_amt else 0 end) as sum_net_cost
,sum(case when SALE_TYPE_IND = 'RETURN' then net_unit_cost_amt else 0 end) as sum_return_cost 
,sum(cnt_class) as cnt_class
from
(SELect     
                                
                                 
								STH.TRANSACTION_DT,
								
            					case when STH.TRANSACTION_DT > '2019-03-29' 
									then (case when STD.SALES_RETURN_IND ='N' then 'SALE' else 'RETURN' end)
									else (case when STD.SALES_QTY>0 THEN 'SALE' else 'RETURN' END)
								end AS SALE_TYPE_IND,                                 
                             	TRIM(CAST(STD.STORE_NBR  AS INT)||'-'||trim(cast(STD.register_nbr as int))||'-'||STD.TRANSACTION_DT||'-'||TRIM(STD.TRANSACTION_NBR))AS txn_key,		
								count(distinct STH.CUSTOMER_NBR) CNT_CUSTOMER_NBR ,
                                count(distinct ISKU.class_nbr) cnt_class,
								case when STH.TRANSACTION_DT > '2019-03-29' 
									then sum(COALESCE(STD.NET_SALES_RTL, 0))
									else SUM(STD.NET_SALES_RTL-STD.RETURN_RTL) end AS RETAIL_SALES_AMT,
									
                                case when STH.TRANSACTION_DT > '2019-03-29' 
									then sum(COALESCE(STD.NET_SALES_QTY, 0))
									else SUM (abs(STD.NET_SALES_QTY)-abs(STD.RETURN_QTY)) end AS RETAIL_QUANTITY_CNT ,
								
							SUM(CASE WHEN  coalesce(STORE_ON_HAND_QTY,0)= 0 THEN round(coalesce(STORE_ON_HAND_CST,0.0),2)
               						 else  round(coalesce(STORE_ON_HAND_CST,0.0)/STORE_ON_HAND_QTY,2) 
          					END) AS net_unit_cost_amt,
								
								SUM(STD.RETURN_RTL ) AS RETURN_SALES_AMT,
                                
								SUM(STD.RETURN_QTY) AS RETURN_QUANTITY_CNT
                        FROM    
                        EDWP_LB_RPT.SALES_TRANSACTION_HEADER STH 
                        INNER JOIN                      EDWP_LB_DATAVIEW.SALES_TRANSACTION_DETAIL STD
                           ON  STH.SELLING_CHAIN_NBR = STD.SELLING_CHAIN_NBR
                                                AND     STH.SELLING_STORE_NBR = STD.SELLING_STORE_NBR
                                                AND     STH.TRANSACTION_DT = STD.TRANSACTION_DT
                                                AND     STH.TRANSACTION_NBR = STD.TRANSACTION_NBR
                                                AND     STH.REGISTER_NBR = STD.REGISTER_NBR
                                                INNER JOIN 
                                                ( sel * From edwp_lb_rpt.item_sku where division_nbr in (500,510,520)) ISKU
                                                ON
                                                --AND   STH.ORDER_NBR IS NOT NULL
                          STD.SKU_NBR=ISKU.SKU_NBR         
						left join EDWP_LB_RPT.ASNA_CALENDAR e
						    on std.TRANSACTION_DT=e.calendar_dt
						left join EDWP_LB_RPT.SKU_STORE_WEEK f
		    				on e.FISCAL_WEEK_LW_NBR=f.FISCAL_WEEK_NBR
		    				and std.STORE_NBR=f.STORE_NBR
		    				and std.SKU_NBR=f.SKU_NBR  
                        WHERE      STD.chain_nbr='7' --and std.selling_chain_nbr = '7'
						AND std.TRANSACTION_DT in ('2019-05-01','2019-05-30','2018-10-01','2018-10-31')   --between '2018-10-01' and '2018-10-31'					
                                GROUP BY 1,2,3)a								
								where RETAIL_SALES_AMT <> 0
								group by 1 order by 1