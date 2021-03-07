
#pk check
bq query --use_legacy_sql=false <<!
select a.if_entry_no 
    ,count(*)
from edl_stage.pre_aw_transaction_header a
group by a.if_entry_no 
having count(*) > 1
limit 10
!

#null check
bq query --use_legacy_sql=false <<!
SELECT count(*) 
FROM    p-asna-analytics-002.edl_stage.pre_aw_transaction_header 
where   if_entry_no is null
!


--transaction line

#null check
bq query --use_legacy_sql=false <<!
SELECT
  if_entry_no
FROM
  p-asna-analytics-002.edl_stage.pre_aw_transaction_line
WHERE
  if_entry_no IS NULL
!

#pk check
bq query --use_legacy_sql=false <<!
SELECT
  if_entry_no,
  line_id
FROM
  p-asna-analytics-002.edl_stage.pre_aw_transaction_line
GROUP BY
  if_entry_no,
  line_id
HAVING
  COUNT(*)>1
!

#fk check: transaction line
bq query --use_legacy_sql=false <<!
select sum(case when b.if_entry_no is null then 0 else 1 end)   as match_count
    ,sum(case when b.if_entry_no is null then 1 else 0 end)     as orphan_count
from edl_stage.pre_aw_transaction_line a 
left join edl_stage.pre_aw_transaction_header b 
    on a.if_entry_no = b.if_entry_no
	
!


bq cp --force edl_archive.pre_transaction_item edl_archive.pre_aw_transaction_linenotes


#fk check: transaction linenotes
bq query --use_legacy_sql=false <<!
select sum(case when b.if_entry_no is null then 0 else 1 end)   as match_count
    ,sum(case when b.if_entry_no is null then 1 else 0 end)     as orphan_count
from edl_stage.pre_aw_transaction_linenotes  a 
left join edl_stage.pre_aw_transaction_header  b 
    on a.if_entry_no = b.if_entry_no
    where a.batch_id>=20180901 and a.batch_id<=20190729
!	
--773013850   0


#pk check
bq query --use_legacy_sql=false <<!
SELECT
  if_entry_no,
  line_id
FROM
  edl_stage.pre_aw_transaction_merch_detail
GROUP BY
  if_entry_no,
  line_id
HAVING
  COUNT(*)>1
  limit 10
!

#null check
bq query --use_legacy_sql=false <<!
SELECT
  count(*)
FROM
  edl_stage.pre_aw_transaction_merch_detail
WHERE
  if_entry_no IS NULL
!

#fk check: transaction merch_detail
bq query --use_legacy_sql=false <<!
select sum(case when b.if_entry_no is null then 0 else 1 end)   as match_count
    ,sum(case when b.if_entry_no is null then 1 else 0 end)     as orphan_count
from edl_stage.pre_aw_transaction_merch_detail   a 
left join edl_stage.pre_aw_transaction_header  b 
    on a.if_entry_no = b.if_entry_no
    where a.batch_id>=20180901 and a.batch_id<=20190729
!        
--117338483  0


#fk check: transaction return_detail
bq query --use_legacy_sql=false <<!
select sum(case when b.if_entry_no is null then 0 else 1 end)   as match_count
    ,sum(case when b.if_entry_no is null then 1 else 0 end)     as orphan_count
from edl_stage.pre_aw_transaction_return_detail    a 
left join edl_stage.pre_aw_transaction_header  b 
    on a.if_entry_no = b.if_entry_no
    where a.batch_id>=20180901 and a.batch_id<=20190729
!        
--32091061  0

#fk check: transaction tax_detail
bq query --use_legacy_sql=false <<!
select sum(case when b.if_entry_no is null then 0 else 1 end)   as match_count
    ,sum(case when b.if_entry_no is null then 1 else 0 end)     as orphan_count
from edl_stage.pre_aw_transaction_tax_detail     a 
left join edl_stage.pre_aw_transaction_header  b 
    on a.if_entry_no = b.if_entry_no
    where a.batch_id>=20180901 and a.batch_id<=20190729
!        
--48233755 0

#fk check: transaction discount_detail
bq query --use_legacy_sql=false <<!
select sum(case when b.if_entry_no is null then 0 else 1 end)   as match_count
    ,sum(case when b.if_entry_no is null then 1 else 0 end)     as orphan_count
from edl_stage.pre_aw_transaction_discount_detail      a 
left join edl_stage.pre_aw_transaction_header  b 
    on a.if_entry_no = b.if_entry_no
    where a.batch_id>=20180901 and a.batch_id<=20190729
!        
--118668358 0
*/



ann_ann_aw_transactions_merchdtl-ut.bq
ann_ann_aw_transactions_linenotes-ut.bq
ann_ann_aw_transactions_discountdtl-ut.bq
ann_ann_aw_transactions_taxdetail-ut.bq
ann_ann_aw_transactions_returndtl-ut.bq
ann_ann_aw_transactions_line-ut.bq
ann_ann_aw_transactions_header-ut.bq