-- select hist.txn_nbr from `p-asna-analytics-002.edl_landing.ann_hst_f_txn_header` as hist group by  hist.txn_nbr having count(*)>1;


There are few cases 

select CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',cast(transaction_no AS string)) ,inc.interface_control_flag  from edl_landing.ann_ann_aw_transactions_header as inc where inc.store_no not in ( 611, 612, 613, 616, 617, 618 , 619)  group by CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',cast(transaction_no AS string)),inc.interface_control_flag having count(*)>1;

-- There are few duplicates

select CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',cast(transaction_no AS string)) ,inc.interface_control_flag  from edl_landing.ann_ann_aw_transactions_header as inc where inc.store_no not in ( 611, 612, 613, 616, 617, 618 , 619)  
and
	CONCAT(CAST(inc.store_no AS string)	,':'
	,CAST (inc.register_no AS string)	,':'
	,FORMAT_DATE('%Y%m%d',transaction_date), ':'
	,cast(transaction_no AS string)) 
	NOT IN 
			(select CONCAT(CAST(inc_20.store_no AS string),':'
					,CAST (inc_20.register_no AS string),':'
					,FORMAT_DATE('%Y%m%d',inc_20.transaction_date), ':'
					,cast(inc_20.transaction_no AS string)) 
			from edl_landing.ann_ann_aw_transactions_header inc_20  
			where inc_20.interface_control_flag  = 20)
group by CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',cast(transaction_no AS string)),inc.interface_control_flag having count(*)>1;

--



select
		   SUM(case when hist.txn_nbr is null then 1 else 0 end) missing_in_hist_tran_cnt 	
		  ,SUM(case when hist.txn_nbr is null then 0 else 1 end) matching_in_hist_tran_cnt 	
          from     edl_landing.ann_ann_aw_transactions_header inc                    
LEFT join (
				SELECT 						
						hist.store_nbr
						,hist.txn_nbr						
				FROM `p-asna-analytics-002.edl_landing.ann_hst_f_txn_header` hist
				where HIST.txn_source_cd ='AW'
			)	hist
			on hist.txn_nbr = CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',cast(transaction_no AS string))
      join edl_landing.ann_calendar cal
  on CAST (day_dt AS date) = inc.transaction_date
  and inc.store_no not in ( 611, 612, 613, 616, 617, 618 , 619) 
  and cal.fiscal_Week_id < 201916
  
-- missing_in_hist_tran_cnt	matching_in_hist_tran_cnt
-- 10767								21019737
 
-----------------------------------------------------------------------------
-- header with only unique record of POS

select CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',cast(transaction_no AS string)) ,inc.interface_control_flag  from edl_landing.ann_ann_aw_transactions_header as inc
LEFT join (
				SELECT 						
						hist.store_nbr
						,hist.txn_nbr						
				FROM `p-asna-analytics-002.edl_landing.ann_hst_f_txn_header` hist
				where HIST.txn_source_cd ='AW'
			)	hist
			on hist.txn_nbr = CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',cast(transaction_no AS string))
join edl_landing.ann_calendar cal
  on CAST (day_dt AS date) = inc.transaction_date
 where inc.store_no not in ( 611, 612, 613, 616, 617, 618 , 619)  
and
	CONCAT(CAST(inc.store_no AS string)	,':'
	,CAST (inc.register_no AS string)	,':'
	,FORMAT_DATE('%Y%m%d',transaction_date), ':'
	,cast(transaction_no AS string)) 
	NOT IN 
			(select CONCAT(CAST(inc_20.store_no AS string),':'
					,CAST (inc_20.register_no AS string),':'
					,FORMAT_DATE('%Y%m%d',inc_20.transaction_date), ':'
					,cast(inc_20.transaction_no AS string)) 
			from edl_landing.ann_ann_aw_transactions_header inc_20  
			where inc_20.interface_control_flag  = 20)
      and hist.txn_nbr not in ('2900:1:20180911:7856','1483:11:20190323:1303')
group by CONCAT(CAST(inc.store_no AS string),':',CAST (inc.register_no AS string),':',FORMAT_DATE('%Y%m%d',transaction_date), ':',cast(transaction_no AS string)),inc.interface_control_flag having count(*)>1;


 