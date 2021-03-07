CREATE OR REPLACE TABLE analytic_mart.pre_sales_sum_settled_detailed AS
select 
                                                cal.fiscal_week_id week_id
                                                ,inc.store_no AS store_id
                                                ,s.style_number AS style_id
                                                ,p.color AS color_id
                                                ,inc.transaction_date -- DAY 
                                                ,p.sku            
      ,sum(CASE
              WHEN l.line_Action = 1
          and gross_line_amount >= 0 THEN 1
              WHEN l.line_Action = 2
          and gross_line_amount < 0 THEN 1
              WHEN l.line_Action = 1
          and gross_line_amount < 0 THEN -1
              WHEN l.line_Action = 2
          and gross_line_amount >= 0 THEN -1
              ELSE 0
              END
          )                                                         AS Net_Sales_Unit_cnt
        ,(ROUND(sum(CASE
                  WHEN l.line_Action = 1
              and gross_line_amount >= 0 THEN gross_line_amount-pos_discount_amount
                  WHEN l.line_Action = 2
              and gross_line_amount < 0 THEN gross_line_amount-pos_discount_amount
                  WHEN l.line_Action = 1
              and gross_line_amount < 0 THEN (-1)*(gross_line_amount-pos_discount_amount)
                  WHEN l.line_Action = 2
              and gross_line_amount >= 0 THEN (-1)*(gross_line_amount-pos_discount_amount)
                  ELSE 0
                  END
              )/100, 2))                                             AS Net_sales_amt,

                                SUM(CASE WHEN l.line_Action = 1 and gross_line_amount < 0   THEN 1
          WHEN l.line_Action = 2 and gross_line_amount >= 0  THEN 1                
                                ELSE    0
                  END    ) AS Return_Sales_Unit_cnt,
                  ROUND(SUM(CASE        
          WHEN l.line_Action = 1 and gross_line_amount < 0  THEN (gross_line_amount-pos_discount_amount) 
          WHEN l.line_Action = 2 and gross_line_amount >= 0 THEN (gross_line_amount-pos_discount_amount)
                                  ELSE      0
                                END      )/100,2) AS return_sales_dollars_amt,
                                ROUND(sum(CASE  WHEN ifnull(full_price_flg,'N') = 'F' THEN 
                                                  (CASE
                                                  WHEN l.line_Action = 1 THEN pos_discount_amount
                                                  WHEN l.line_Action = 2 THEN (-1)*(if(pos_discount_amount=0,1,pos_discount_amount))
                                                ELSE        0
                                  END        )
                                END      )/100,2) AS Promotion_Net_Sales_Dollars_amt ,
                  SUM(CASE WHEN ifnull(full_price_flg,'N') = 'F' THEN 
                                  (CASE
                                                WHEN l.line_Action = 1 THEN 1
                                                WHEN l.line_Action = 2 THEN -1
                                                ELSE 0
                                   END)
                                  end) AS Promotion_Net_Sales_Unit_cnt,
                                   ROUND(sum(CASE   WHEN ifnull(full_price_flg,'N') <> 'F' THEN (CASE
                                                  WHEN l.line_Action = 1 THEN gross_line_amount-pos_discount_amount
                                                  WHEN l.line_Action = 2 THEN (-1)*(gross_line_amount-pos_discount_amount)
                                                ELSE        0
                                  END        )
                                END      )/100,2) AS Markdown_Net_Sales_Dollars_amt ,
                  SUM(CASE WHEN  ifnull(full_price_flg,'N') <> 'F' THEN 
                                  (CASE
                                                WHEN l.line_Action = 1 THEN 1
                                                WHEN l.line_Action = 2 THEN -1
                                                ELSE 0
                                   END)
                                  end) AS Markdown_Net_Sales_Unit_cnt
                                
          from     edl_landing.ann_ann_aw_transactions_header inc      
          JOIN edl_landing.ann_ann_aw_transactions_line l
              ON inc.if_entry_no = l.if_entry_no
              AND l.line_object_type = 1
              AND line_void_flag = '0'
         left join edl_landing.ann_ann_aw_transactions_merchdtl m
              on m.if_entry_no = l.if_entry_no
              and l.line_id = m.line_id
         left JOIN edl_landing.ann_calendar cal
              ON transaction_Date = CAST (day_dt AS date)
         left JOIN (
               SELECT *
                 FROM (
                          (
                           SELECT *
                                , ROW_NUMBER()
                             OVER (
                            PARTITION BY sku
                                ORDER BY datetime DESC
                                  ) AS rn
                             FROM edl_landing.ann_ann_sap_product
                          )
                      )a
                WHERE rn=1
              )p
           ON cast(m.sku_id as string) = p.sku
         left JOIN edl_landing.ann_ann_sap_style s
           ON s.style_number = substr(p.article, 10, 6) 
                                LEFT JOIN ( SELECT *       FROM 
                                                                (SELECT *,
                                                                                                ROW_NUMBER() OVER (PARTITION BY transaction_nbr, item_sku, full_price_flg, batch_id) AS rn
                                                                FROM work.ann_ann_bi_transaction_fpnfp ) a
                                                                WHERE rn = 1 
                                                )bi
                                ON  bi.item_sku = cast(m.sku_id as string)            
                                AND bi.transaction_nbr = CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',SUBSTR(CAST(transaction_no AS string),1,4))                     
where 
                CONCAT(CAST(inc.store_no AS string)     ,':'
                ,CAST (inc.register_no AS string) ,':'
                ,FORMAT_DATE('%Y%m%d',transaction_date), ':'
                ,cast(transaction_no AS string)) 
                NOT IN 
                                                (select CONCAT(CAST(inc_20.store_no AS string),':'
                                                                                ,CAST (inc_20.register_no AS string),':'
                                                                                ,FORMAT_DATE('%Y%m%d',inc_20.transaction_date), ':'
                                                                                ,cast(inc_20.transaction_no AS string)) 
                                                from edl_landing.ann_ann_aw_transactions_header inc_20  
                                                where inc_20.interface_control_flag  = 20)
and inc.store_no not in ( 611, 612, 613, 616, 617, 618 , 619)   
group by 1,2,3,4,5,6
