set hivevar:days=${p1};


insert overwrite table ascena_analytic_mart.ann_ann_atg_returns
select   returnid
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
 ,sumoftotalrefundamount
 ,method
 ,shipping_label_fee
 ,batch_id
from ascena_staging.ann_ann_atg_returns
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) > date_sub(current_date,${days})
;