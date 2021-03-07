-- add the lead logic
create or replace table work.Style_Store_Cost as
WITH
  unit_amt AS (
  SELECT
    chain_id,
    store_id,
    week_id,
    style_id,
    color_id,
    unit_cogs_amt,
    prev_week_cogs_amt,
    prev_week_cogs_cnt
  FROM (
    SELECT
      chain_id,
      store_id,
      week_id,
      style_id,
      color_id,
      ROUND(SUM(bop_store_inv_actual_cost_amt/bop_store_inv_unit_cnt),2) AS unit_cogs_amt,
      ROW_NUMBER() OVER (PARTITION BY chain_id, store_id, style_id, color_id ORDER BY week_id DESC ) rn,
	    LEAD (ROUND(SUM(bop_store_inv_actual_cost_amt/bop_store_inv_unit_cnt),2)) 
					over (partition by s.chain_id,s.store_id,s.style_id,s.color_id order by s.week_id desc) prev_week_cogs_amt,
      LEAD (SUM(bop_store_inv_unit_cnt)) 
					over (partition by s.chain_id,s.store_id,s.style_id,s.color_id order by s.week_id desc) prev_week_cogs_cnt    
    FROM
      demand_forecast.inventory_store s
    WHERE      
      bop_store_inv_unit_cnt <> 0
    GROUP BY
      chain_id,
      store_id,
      week_id,
      style_id,
      color_id)
  WHERE
    rn = 1)
SELECT
  s.chain_id,
  s.store_id,
  s.week_id,
  s.style_id,
  s.color_id
 , case when sum(bop_store_inv_unit_cnt) <> 0 then ROUND(SUM(bop_store_inv_actual_cost_amt)/sum(bop_store_inv_unit_cnt),2)
          when  sum(prev_week_cogs_cnt) <> 0 then sum(prev_week_cogs_amt)
				else sum(u.unit_cogs_amt)
    end as unit_cost_amt
FROM
  demand_forecast.inventory_store s  
left join unit_amt  u
  on u.chain_id = s.chain_id
  and u.store_id = s.store_id
  and u.style_id = s.style_id
  and u.color_id = s.color_id
 -- where s.store_id = 437 and s.style_id like '%473021%' and s.color_id like ('%2222')
GROUP BY
  s.chain_id,
  s.store_id,
  s.week_id,
  s.style_id,
  s.color_id