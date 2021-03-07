CREATE OR REPLACE TABLE analytic_mart.pre_transaction_header
AS
select 
	 FARM_FINGERPRINT(replace(txn_nbr,':', '|')) as transaction_key
	,TXN_NBR as transaction_num
	,TXN_SOURCE_CD as transaction_source_cd
	,TXN_STATUS_CD as transaction_status_cd
	,FARM_FINGERPRINT(cast(STORE_NBR as string)) as store_key
	,REGISTER_NBR as register_num
	,BRAND_CD as brand_cd
	,EXTRACT(DATE FROM TXN_DT) as transaction_dt
	,EXTRACT(TIME FROM TXN_DT) as transaction_tms
	,TXN_CHANNEL_CD as channel_cd 
	,CASE WHEN txn_type_cd='PUR' THEN ROUND(TXN_NET_AMT,2)
        WHEN txn_type_cd='RTN' THEN ROUND(txn_ret_net_amt,2)
        WHEN txn_type_cd='EXC' THEN ROUND(ROUND(txn_net_amt,2)-ROUND(txn_ret_net_amt,2),2)
        ELSE NULL END
        as transaction_net_amt
   ,CASE WHEN txn_type_cd='PUR' THEN ROUND(TXN_TAX_AMT,2)
        WHEN txn_type_cd='RTN' THEN ROUND(txn_ret_tax_amt,2)
        WHEN txn_type_cd='EXC' THEN ROUND(ROUND(TXN_TAX_AMT,2)-ROUND(txn_ret_tax_amt,2),2)
        ELSE NULL END
	as transaction_tax_amt
	,NULL as tender_total_amt
	,'20160101' as batch_id
from edl_stage.pre_hst_transaction_header 
where EXTRACT(DATE FROM txn_dt)  between '2016-01-01' and '2019-05-04'
union
select
transaction_key
,transaction_num
,'AW' as transaction_source_cd
,NULL as transaction_status_cd
,store_key
,register_num
,s.brand_cd as brand_cd
,transaction_dt
,transaction_tms
,channel_cd
,NULL as transaction_net_amt
,NULL as transaction_tax_amt
,tender_total_amt
,batch_id
from
(
		select
		 FARM_FINGERPRINT(concat(CAST(store_no AS string),'|'
		,CAST (register_no AS string),'|'
		,CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING),'|'
		,cast(transaction_no AS string))) as transaction_key,
		CONCAT(CAST(store_no AS string),'|'
		,CAST (register_no AS string),'|'
		,CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING),'|'
		,cast(transaction_no AS string)) as transaction_num,
		 'AW' as transaction_source_cd
		,NULL as transaction_status_cd
		,FARM_FINGERPRINT(cast(store_no as string)) as store_key
		,store_no
		,register_no as register_num
		,entry_date_time
		,EXTRACT(DATE FROM entry_date_time) as transaction_dt
		,EXTRACT(TIME FROM entry_date_time) as transaction_tms
		,'RET' as channel_cd
		,tender_total as tender_total_amt
		,batch_id
		,row_number() over(
				partition by CONCAT(CAST(store_no AS string),'|'
									,CAST (register_no AS string),'|'
									,CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING),'|'
									,cast(transaction_no AS string)) 
										order by if_entry_no desc) as rn
									from edl_stage.pre_aw_transaction_header
	    where	store_no not in (611,612,613,616,617)
		and EXTRACT(DATE FROM entry_date_time)>'2019-05-04'
	    and 
	        CONCAT(CAST(store_no AS string),'|'
				  ,CAST (register_no AS string),'|'
				  ,CAST(FORMAT_TIMESTAMP("%Y%m%d", entry_date_time) AS STRING),'|'
				  ,CAST(transaction_no AS string))
	                    NOT IN 
						    (select	CONCAT(CAST(tmp.store_no AS string),'|'
		                              ,CAST (tmp.register_no AS string),'|'
		                              ,CAST(FORMAT_TIMESTAMP("%Y%m%d",tmp.entry_date_time) AS STRING),'|'
		                              ,cast(tmp.transaction_no AS string)) 
		                     from	edl_stage.pre_aw_transaction_header tmp  
		                     where	tmp.interface_control_flag  = 20)
	  ) as h
left outer join
(select	store_no, 
		brand,
        case 
			when distribution_channel = '05' and region = '0030' then 'LNG' 
			when distribution_channel in ('02','03') then 'AT' 
			when distribution_channel in ('04','08') then 'ATF'
            when distribution_channel in ('05','06') then 'LOFT' 
			when distribution_channel in ('07','09') then 'LOS' 
			when brand = '99' THEN 'ENT' 
			else NULL 
		end as brand_cd 
        FROM edl_stage.pre_sap_store 
    WHERE store_no NOT LIKE 'A%' 
	AND store_no NOT LIKE 'R%') s 
	on (h.store_no = CAST(s.store_no AS INT64))       
where	rn=1
union
select FARM_FINGERPRINT(ord.orderid) as transaction_key
		,ord.orderid as transaction_num
		,'ATG' AS  transaction_source_cd
		,ord.orderstate as  transaction_status_cd
		,FARM_FINGERPRINT(cast(ord.siteid as string)) as store_key
		,1 as register_num
		,s.brand_cd as brand_cd
		,EXTRACT(DATE FROM (ord.submitteddate)) as transaction_dt
		,EXTRACT(TIME FROM ord.submitteddate) as transaction_tms
		,'WEB' as channel_cd
		,ord.priceinfoamount as transaction_net_amt
		,ord.orderpriceinfotax as transaction_tax_amt
		,NULL as tender_total_amt
		,ord.batch_id
from edl_stage.pre_atg_order_header_curr ord
left outer join
(select	store_no, 
		brand,
        case 
			when distribution_channel = '05' and region = '0030' then 'LNG' 
			when distribution_channel in ('02','03') then 'AT' 
			when distribution_channel in ('04','08') then 'ATF'
            when distribution_channel in ('05','06') then 'LOFT' 
			when distribution_channel in ('07','09') then 'LOS' 
			when brand = '99' THEN 'ENT' 
			else NULL 
		end as brand_cd 
        FROM edl_stage.pre_sap_store 
    WHERE store_no NOT LIKE 'A%' 
	AND store_no NOT LIKE 'R%') s 
	on (cast(ord.siteid as INT64)= CAST(s.store_no AS INT64))
  and EXTRACT(DATE FROM (submitteddate)) > '2019-05-04'
  
union
select FARM_FINGERPRINT(ret.returnid) as transaction_key
		,ret.returnid as transaction_num
		,'ATG' AS  transaction_source_cd
		,ord.orderstate as  transaction_status_cd
		,FARM_FINGERPRINT(cast(ord.siteid as string)) as store_key
		,1 as register_num
		,s.brand_cd as brand_cd
		,EXTRACT(DATE FROM (ret.createddate)) as transaction_dt
		,EXTRACT(TIME FROM ret.createddate) as transaction_tms
		,'WEB' as channel_cd
		,ret.transaction_net_amt
		,ret.transaction_tax_amt
		,NULL as tender_total_amt
		,ret.batch_id
from   (select
 orderid,
 returnid,
 createddate,
 sum(refundamount) as transaction_net_amt,
 actualtaxrefund as transaction_tax_amt,
 batch_id
 from
 (SELECT distinct 
returnid
,orderid
,replacementorderid
,createddate
,actualtaxrefund
,actualshiprefund
,otherrefund
,processimmediately
,processed
,originofreturns
,returnfee
,commerceitemid
,shippinggroupid
,quantitytoreturn
,quantitytoreplace
,isreturnshippingrequired
,quantityreceived
,refundamount
,actualtaxrefunditem
,actualshiprefunditem
,bonusrefund
,shipping_label_fee
,batch_id
from edl_stage.pre_atg_order_return 
)
group by 1,2,3,5,6) as ret
join   edl_stage.pre_atg_order_header ord
       on ret.orderid = ord.orderid
left outer join
(select	store_no, 
		brand,
        case 
			when distribution_channel = '05' and region = '0030' then 'LNG' 
			when distribution_channel in ('02','03') then 'AT' 
			when distribution_channel in ('04','08') then 'ATF'
            when distribution_channel in ('05','06') then 'LOFT' 
			when distribution_channel in ('07','09') then 'LOS' 
			when brand = '99' THEN 'ENT' 
			else NULL 
		end as brand_cd 
        FROM edl_stage.pre_sap_store 
    WHERE store_no NOT LIKE 'A%' 
	AND store_no NOT LIKE 'R%') s 
	on (cast(ord.siteid as INT64)= CAST(s.store_no AS INT64))
  where 
  EXTRACT(DATE FROM (ret.createddate)) > '2019-05-04' 