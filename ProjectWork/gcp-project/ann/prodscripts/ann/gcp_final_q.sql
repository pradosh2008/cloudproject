--create or replace view `p-asna-analytics-002.work.gcp_oct_cust_rj` as
select 
  --brand_customer_id
  transaction_dt
  --,count(distinct brand_customer_id) cnt_cust
  ,sum(CNT_CUSTOMER_NBR) as cnt_customer_nbr
  ,count( distinct txn_key) cnt_txn
  ,sum(case when SALE_TYPE_IND = 'SALE' then net_sales else 0 end) as sum_sales
  ,sum(case when SALE_TYPE_IND = 'SALE' then net_qty else 0 end) as sum_qty  
   ,sum(case when SALE_TYPE_IND = 'RETURN' then -1*return_sls else 0 end) as RETURN_SALES_AMT
  ,sum(case when SALE_TYPE_IND = 'RETURN' then net_qty else 0 end) as RETURN_QUANTITY_CNT
  ,sum(case when SALE_TYPE_IND = 'SALE' then net_unit_cost_amt else 0 end) as sum_net_cost
  ,sum(case when SALE_TYPE_IND = 'RETURN' then net_unit_cost_amt else 0 end) as sum_return_cost 
  ,sum(cnt_class) as cnt_class
   from
  (SELECT
        d.transaction_dt
        
        ,(case when d.transaction_dt > '2019-03-28' 
                then (case when d.return_ind = 1 
                        then 'RETURN' 
                        else 'SALE' end)
             else (case when d.sku_retail_amt > 0 
                        THEN 'SALE'
                        else 'RETURN' end)end) AS SALE_TYPE_IND
        ,concat(s.store_id ,"-", cast(h.register_num as string),"-",cast(h.TRANSACTION_DT as string),"-",cast(h.transaction_num as string)) txn_key
        ,count(distinct c.brand_customer_id ) as CNT_CUSTOMER_NBR
        ,(case when d.transaction_dt > '2019-03-28' 
                    then sum(d.sku_retail_amt)
              else sum ((d.sku_retail_amt*d.sku_quantity_num) + (d.markdown_amt*d.sku_quantity_num) )
              end) as net_sales      
        ,case when d.transaction_dt > '2019-03-28' 
                    then sum(d.sku_retail_amt*d.sku_quantity_num)
              else sum ((d.sku_retail_amt*d.sku_quantity_num) + (d.markdown_amt*d.sku_quantity_num) )
              end as return_sls              
        ,sum(COALESCE(d.sku_quantity_num, 0)) as net_qty

,       sum(CASE WHEN COALESCE(cast(inv.BOP_STORE_INV_U as INT64),0)=0 
                                    THEN round(COALESCE(cast(inv.BOP_STORE_INV_C_ACTUAL as NUMERIC), 0),2)
                                else round(COALESCE(cast(inv.BOP_STORE_INV_C_ACTUAL as NUMERIC), 0)/COALESCE(cast(inv.BOP_STORE_INV_U as INT64),0),2)                 
                                    end) as net_unit_cost_amt
        , count(distinct i.class_id) cnt_class     
        from    `p-asna-analytics-002.analytic_mart.fact_transaction_detail` d
        join `p-asna-analytics-002.analytic_mart.fact_transaction`  h
        on h.transaction_key = d.transaction_key         
        left join `p-asna-analytics-002.analytic_mart.dim_brand_customer` c
        on c.brand_customer_key = d.brand_customer_key 
        join `p-asna-analytics-002.analytic_mart.dim_store` s
        on s.store_key = h.store_key
        JOIN (select * from `p-asna-analytics-002.analytic_mart.dim_item` i ) i
                   ON i.item_key = d.item_key           
                    and  i.division_id in ('500','510','520')
      -- cogs
      LEFT OUTER JOIN edl_conform.plus_calendar cal
     ON cal.calendar_dt = h.transaction_dt
     LEFT OUTER JOIN (SELECT STORE_ID, WEEK_ID, SKU_ID, BOP_STORE_INV_U, BOP_STORE_INV_C_ACTUAL FROM edl_stage.plus_inventory_store_sku_lb) inv
     ON inv.STORE_ID = CONCAT('350', s.store_id) AND CAST(inv.WEEK_ID AS INT64) = cal.fiscal_week_nbr AND CAST(inv.SKU_ID AS INT64) = CAST(i.sku_id AS INT64)                    
        where d.transaction_dt in ( '2019-05-01','2019-05-30','2018-10-01','2018-10-31') 
        and d.brand_cd = 'LB' 
        AND COALESCE(d.gift_card_ind, 0) = 0     
             AND COALESCE(d.line_object_cd, '') NOT IN ('110') 
                  group by 1,2,3
                )
                 where  net_sales !=0
                 group by 1
  order by 1