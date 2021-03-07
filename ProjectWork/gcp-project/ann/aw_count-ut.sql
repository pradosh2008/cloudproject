--aw header

select count(*) from ascena_staging.ann_ann_aw_transactions_header
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('2018-09-01' as string),'yyyy-MM-dd')),'yyyy-MM-dd') --42053120

select count(*) from ascena_staging.ann_ann_aw_transactions_header
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('2018-09-01' as string),'yyyy-MM-dd')),'yyyy-MM-dd') and to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) < date_format(from_unixtime(unix_timestamp(cast('2019-10-08' as string),'yyyy-MM-dd')),'yyyy-MM-dd')


SELECT count(*) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_header` LIMIT 1000  --42053120
SELECT max(batch_id) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_header` LIMIT 1000


--aw line

select count(*) from ascena_staging.ann_ann_aw_transactions_line
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('2018-09-01' as string),'yyyy-MM-dd')),'yyyy-MM-dd') and to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) < date_format(from_unixtime(unix_timestamp(cast('2019-10-08' as string),'yyyy-MM-dd')),'yyyy-MM-dd') --521405596


SELECT count(*) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_line` LIMIT 1000  --521405596
SELECT max(batch_id) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_line` LIMIT 1000 

--aw merch detail

select count(*) from ascena_staging.ann_ann_aw_transactions_merchdtl
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('2018-09-01' as string),'yyyy-MM-dd')),'yyyy-MM-dd') and to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) < date_format(from_unixtime(unix_timestamp(cast('2019-10-08' as string),'yyyy-MM-dd')),'yyyy-MM-dd') --130338383


SELECT count(*) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_merch_detail`  --130338383
SELECT max(batch_id) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_merch_detail`

--aw return detail

select count(*) from ascena_staging.ann_ann_aw_transactions_returndtl
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('2018-09-01' as string),'yyyy-MM-dd')),'yyyy-MM-dd')  and to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) < date_format(from_unixtime(unix_timestamp(cast('2019-10-08' as string),'yyyy-MM-dd')),'yyyy-MM-dd')  --35256462


SELECT count(*) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_return_detail`  --35256462
SELECT max(batch_id) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_return_detail`

--aw tax detail

select count(*) from ascena_staging.ann_ann_aw_transactions_taxdetail
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) between date_format(from_unixtime(unix_timestamp(cast('2018-09-01' as string),'yyyy-MM-dd')),'yyyy-MM-dd') and date_format(from_unixtime(unix_timestamp(cast('2019-10-07' as string),'yyyy-MM-dd')),'yyyy-MM-dd')  --53286697


SELECT count(*) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_tax_detail`  --49050940
SELECT max(batch_id) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_tax_detail`


--aw discount detail

select count(*) from ascena_staging.ann_ann_aw_transactions_discountdtl
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('2018-09-01' as string),'yyyy-MM-dd')),'yyyy-MM-dd') and to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) < date_format(from_unixtime(unix_timestamp(cast('2019-10-08' as string),'yyyy-MM-dd')),'yyyy-MM-dd') --133324020


SELECT count(*) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_discount_detail` LIMIT 1000 --133324020
SELECT max(batch_id) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_discount_detail` LIMIT 1000

--aw linenotes 

select count(*) from ascena_staging.ann_ann_aw_transactions_linenotes
where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('2018-07-01' as string),'yyyy-MM-dd')),'yyyy-MM-dd') and to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) < date_format(from_unixtime(unix_timestamp(cast('2019-10-08' as string),'yyyy-MM-dd')),'yyyy-MM-dd') --969485241 --20190910


SELECT count(*) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_linenotes` LIMIT 1000 --966063095
SELECT max(batch_id) FROM `p-asna-analytics-002.edl_stage.pre_aw_transaction_linenotes` LIMIT 1000

