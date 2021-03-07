SELECT
  h.store_no AS store_id
  ,cal.week_id as week_id
  --,style_number as style_id
  ,  SUM(CASE      WHEN l.line_action = 1 and l.line_object_type = 1  THEN 1    
                  WHEN (LINE_OBJECT_TYPE = 2 AND LINE_ACTION = 11) OR (LINE_OBJECT_TYPE = 6 AND LINE_ACTION = 24 AND LINE_OBJECT in (604, 615, 616, 618, 693)) THEN 1
                  ELSE 0 
                    END    ) AS Net_Sales_Unit_cnt
    ,  SUM(CASE      WHEN l.line_action = 1 and l.line_object_type = 1  THEN round(L.gross_line_amount,2)
                  WHEN (LINE_OBJECT_TYPE = 2 AND LINE_ACTION = 11) OR (LINE_OBJECT_TYPE = 6 AND LINE_ACTION = 24 AND LINE_OBJECT in (604, 615, 616, 618, 693)) THEN 1
                  ELSE 0 
                    END    ) AS Gross_Sales_AMT
  ,  SUM(CASE      WHEN l.line_action = 1 and l.line_object_type = 1  THEN round(gross_line_amount,2)- round(pos_discount_amount,2)* (-1)*db_cr_none
                  WHEN (LINE_OBJECT_TYPE = 2 AND LINE_ACTION = 11) OR (LINE_OBJECT_TYPE = 6 AND LINE_ACTION = 24 AND LINE_OBJECT in (604, 615, 616, 618, 693)) THEN 1
                  ELSE 0 
                    END    ) AS Net_Sales_AMT                    
                    
    /*SUM(CASE      WHEN l.line_action = 1 THEN ROUND(m.sold_at_price/100,2)    ELSE    0  END    ) AS Net_Sales_Amt_merch    
  --,sum(case when l.line_action = 1 then m. else 0 end) as Net_Sales_COGS_amt  
  ,SUM(round(h.tender_total/100,2)) AS tender_total,
  SUM(round(l.gross_line_amount/100,2)) AS gross_line_amount,
  sum(round(gross_line_amount,2)- round(pos_discount_amount,2)* (-1)*db_cr_none)  as paid_sum,
  sum(case when l.line_action = 1 then round(gross_line_amount,2)- round(pos_discount_amount,2)* (-1)*db_cr_none else 0 end)  as paid_sum_1
  */
  
  --SUM(CASE  WHEN l.line_action = 1 THEN ROUND(m.sold_at_price/100,2)    ELSE    0  END    ) AS Net_Sales_Amt  
  --SUM(CASE      WHEN l.line_action = 2 THEN 1    ELSE    0  END    ) AS Return_Sales_Unit_cnt,
  --,sum(case when l.line_action = 2 then m. else 0 end) as Return_Sales_COGS_amt
FROM
  edl_landing.ann_ann_aw_transactions_header h
JOIN
  edl_landing.ann_ann_aw_transactions_line l
ON
  h.if_entry_no = l.if_entry_no
  AND l.line_object_type IN (1,    2,    5,    6)  and line_void_flag='0'
  join edl_landing.ann_ann_aw_transactions_merchdtl m
  on l.if_entry_no = m.if_entry_no
  and l.line_id = m.line_id
  /*join edl_landing.ann_ann_aw_transactions_merchdtl m_ret
on l.if_entry_no = m_ret.if_entry_no
and l.line_id = m_ret.line_id
and l.line_Action = 2 */
JOIN
  edl_landing.ann_calendar cal
ON
  transaction_Date = CAST (day_dt AS date)
/*JOIN (  SELECT
    sku,
    article
  FROM ((
      SELECT
        sku,
        article,
        ROW_NUMBER() OVER(PARTITION BY sku, article ORDER BY date_time DESC) AS rn
      FROM
        edl_landing.ann_ann_sap_product ))a
  WHERE
    rn=1)p
ON
  CAST(m.sku_id AS string ) = p.sku
JOIN
  edl_landing.ann_ann_sap_style s
ON
  s.style_number = substr (p.article,    10,    6) */
WHERE
  --h.if_entry_no = 1019766802 and
  h.store_no = 1206
  AND week_id = 201903
GROUP BY
  h.store_no
  ,cal.week_id
  --,style_number
