set hivevar:days=${p1};

select distinct
batch_id
,to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd'))
FROM ascena_staging.ann_ann_aw_transactions_line
WHERE to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('${days}' as string),'yyyy-MM-dd')),'yyyy-MM-dd') order by batch_id desc ;


select * from ascena_staging.ANN_ANN_AW_TRANSACTIONS_HEADER;
select * from ascena_staging.ANN_ANN_AW_TRANSACTIONS_RETURNDTL;
select * from ascena_staging.ANN_ANN_AW_TRANSACTIONS_MERCHDTL;

select min(batch_id),max(batch_id) from ascena_analytic_mart.ANN_ANN_AW_TRANSACTIONS_HEADER;
-- 20180901  | 20190612

select count(*) from ascena_analytic_mart.ANN_ANN_AW_TRANSACTIONS_HEADER where batch_id>=20180901
--32136258


select min(batch_id),max(batch_id) from ascena_analytic_mart.ANN_ANN_AW_TRANSACTIONS_RETURNDTL;
-- 20180901  | 20190612

select count(*) from ascena_analytic_mart.ANN_ANN_AW_TRANSACTIONS_RETURNDTL where batch_id>=20180901

--27182739

select min(batch_id),max(batch_id) from ascena_analytic_mart.ANN_ANN_AW_TRANSACTIONS_MERCHDTL;


select count(*) from ascena_analytic_mart.ANN_ANN_AW_TRANSACTIONS_MERCHDTL where batch_id>=20180901



gsutil ls -l gs://p-asna-datasink-003/ann_ann_atg_transactions/	
gsutil -m rm -r gs://p-asna-datasink-003/ann_ann_atg_transactions/	


-rw-r--r-- 1 gcpinteg users 100363373 Feb  3 07:01 
-rw-r--r-- 1 gcpinteg users  42312625 Feb  6 04:11 
-rw-r--r-- 1 gcpinteg users   3928075 Feb  3 07:00 
-rw-r--r-- 1 gcpinteg users   1359224 Feb  3 07:00 
-rw-r--r-- 1 gcpinteg users   8492847 Feb  3 07:00 
-rw-r--r-- 1 gcpinteg users 162415959 Feb  3 07:00 
-rw-r--r-- 1 gcpinteg users  73909791 Feb  3 07:02 
-rw-r--r-- 1 gcpinteg users   9989281 Feb  3 07:00 
-rw-r--r-- 1 gcpinteg users   2395584 Feb  3 07:02 
-rw-r--r-- 1 gcpinteg users    197260 Feb  6 04:11 


lb_item_sku20200203.txt.gz
lb_sales_transaction_detail20200206.txt.gz
lb_sales_transaction_discount20200203.txt.gz
lb_sales_transaction_fee20200203.txt.gz
lb_sales_transaction_header20200203.txt.gz
lb_sales_transaction_notes20200203.txt.gz
lb_sales_transaction_tax20200203.txt.gz
lb_sales_transaction_tender20200203.txt.gz
lb_sku_attr_channel20200203.txt.gz
lb_store20200206.txt.gz

gsutil cp /data/plus/edw/lb_item_sku*.txt.gz gs://p-ascena-aadp-landing-01/plus/edw
gsutil cp /data/plus/edw/lb_sales_transaction_discount*.txt.gz gs://p-ascena-aadp-landing-01/plus/edw
gsutil cp /data/plus/edw/lb_sales_transaction_notes*.txt.gz gs://p-ascena-aadp-landing-01/plus/edw
gsutil cp /data/plus/edw/lb_sales_transaction_tender*.txt.gz gs://p-ascena-aadp-landing-01/plus/edw
gsutil cp /data/plus/edw/lb_sales_transaction_tax*.txt.gz gs://p-ascena-aadp-landing-01/plus/edw


bq query --use_legacy_sql=false 'drop table `p-asna-analytics-002.edl_landing.ann_ann_atg_transactions`'
gscp -d ascena_analytic_mart ann_ann_atg_transactions>ann_ann_atg_transactions.out 2>&1
bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.ann_ann_atg_transactions  'gs://p-asna-datasink-003/ann_ann_atg_transactions/*'


gsutil ls -l gs://p-asna-datasink-003/ann_ann_atg_returns/	
gsutil -m rm -r gs://p-asna-datasink-003/ann_ann_atg_transactions/	


gscp -d ascena_analytic_mart ann_ann_atg_returns>ann_ann_atg_returns.out 2>&1

bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.ann_ann_atg_returns  'gs://p-asna-datasink-003/ann_ann_atg_returns/*'



gsutil -m rm -r gs://p-asna-datasink-003/ann_ann_aw_transactions_header/



gscp -d ascena_analytic_mart ann_ann_aw_transactions_header>ann_ann_aw_transactions_header.out 2>&1 &
gscp -d ascena_analytic_mart ann_ann_aw_transactions_returndtl>ann_ann_aw_transactions_returndtl.out 2>&1 &
gscp -d ascena_analytic_mart ann_ann_aw_transactions_merchdtl>ann_ann_aw_transactions_merchdtl.out 2>&1 &

gscp -d ascena_analytic_mart ann_calendar>ann_calendar.out 2>&1 &


gscp -rd ascena_analytic_mart ann_ann_atg_transactions>ann_ann_atg_transactions.out 2>&1 &
gscp -rd ascena_analytic_mart ann_ann_atg_returns>ann_ann_atg_returns.out 2>&1



bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.ann_ann_aw_transactions_header  'gs://p-asna-datasink-003/ann_ann_aw_transactions_header/*'

bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.ann_ann_aw_transactions_returndtl  'gs://p-asna-datasink-003/ann_ann_aw_transactions_returndtl/*'


bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.ann_ann_aw_transactions_merchdtl  'gs://p-asna-datasink-003/ann_ann_aw_transactions_merchdtl/*'

bq load --replace=true --source_format=AVRO --use_avro_logical_types=true edl_landing.ann_calendar  'gs://p-asna-datasink-003/ann_calendar/*'




SELECT
  max(FORMAT_DATETIME("%S", transaction_tms  )),min(FORMAT_DATETIME("%H", transaction_tms  ))
  AS formatted from `edl_conform.transaction`;
  
  
  
------------------------------------
  
  
  --SELECT store_no, register_no, transaction_date, transaction_no FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header` group by store_no, register_no, transaction_date, transaction_no having count(*)>1;

select * from `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header` where
store_no=612
and register_no=1
and transaction_date='2019-03-01'
and transaction_no=6021;
  
--SELECT * FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header` where store_no=1260 and register_no=2 and transaction_no=7589;


SELECT store_no, register_no, entry_date_time, transaction_no, interface_control_flag
FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header` 
group by store_no, register_no, entry_date_time, transaction_no, interface_control_flag having count(*)>1
;


select * from `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header` where 
store_no=1484 and 
register_no=3 and 
entry_date_time='09/09/2018 14:10:00' and
transaction_no=5544
 ;
 
 SELECT store_no, register_no, transaction_date, transaction_no, entry_date_time, interface_control_flag FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header`  group by store_no, register_no, transaction_date,entry_date_time, transaction_no,interface_control_flag having count(*)>1;

select * from `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header` where 
store_no=1764 and
register_no=1 and
transaction_date='2019-02-21' and
transaction_no=13209 and
entry_date_time='02/21/2019 18:21:59' and
interface_control_flag=20;


SELECT store_no, register_no, transaction_date, transaction_no FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header` where store_no > 618 or store_no < 611 group by store_no, register_no, transaction_date, transaction_no having count(*)>1;

--where store_no =1764 or store_no < 611
  
SELECT store_no, register_no, transaction_date, transaction_no FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_header` where
store_no=1521 group by store_no, register_no, transaction_date, transaction_no having count(*)>1 ;

 1045785857
,1045415371
,1045785855
,1045785858
,1045785856


------------------------------------------------------------------------------------------------
SELECT pos_discount_level FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_discountdtl` group by pos_discount_level; --16



--PRIMARY KEY for discountdtl table
 SELECT if_entry_no, line_id, applied_by_line_id, count FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_discountdtl` group by 
if_entry_no, line_id, applied_by_line_id having count>1 



------------------------------------------------------------------------------
product analysis
----------------
--SELECT sku,count(*) FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_product` group by sku having count(*)>1;
--select * from `p-asna-analytics-002.edl_landing.ann_ann_sap_product` where sku='22717535';
select sku,count(*) from
(SELECT sku,row_number() over(partition by sku 
                           order by date_time desc
                        ) as rn FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_product`) as a where rn=1
                        group by sku having count(*)>1;
						

select * from
(SELECT *,row_number() over(partition by sku 
                           order by date_time desc
                        ) as rn FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_product`) as a where rn=1 and sku='22717535'
                    ;
                        
						

-------------------------------------------------------------------------------
merch details

SELECT count(*) FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_merchdtl`;

--99858091

-- latest sku


SELECT count(*) FROM `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_merchdtl` as m inner join 
(select sku from
(SELECT sku,row_number() over(partition by sku 
                           order by date_time desc
                        ) as rn FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_product`) as a where rn=1 ) as p
                        on cast(m.sku_id as string) = p.sku ;
					
					

--------------------------------------------------------------


select 
l.if_entry_no
,l.interface_control_flag
,l.record_type
,l.line_id
,l.line_object_type
,l.line_object
,l.line_action
,m.record_type as mrectype
,m.line_id as mlineid
,m.merchandise_category
,m.upc_lookup_division
,m.upc_no
,m.sku_id
,m.style_reference_id
,m.class_code
,m.subclass_code
,m.pos_identifier_type
,m.pos_identifier
,m.pos_dept_class
,m.units
,m.salesperson
,m.salesperson2

from `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_line` as l inner join
`p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_merchdtl` as m on l.if_entry_no =m.if_entry_no and l.line_id =m.line_id  where l.if_entry_no=1046259770 ;

-- This gave me line object type 1


why ???

select 
l.line_object_type 
from `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_line` as l inner join
`p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_merchdtl` as m on l.if_entry_no =m.if_entry_no and l.line_id =m.line_id group by l.line_object_type ;

6
1
5
0
2


----------------when it is 6

select 
l.if_entry_no
,l.interface_control_flag
,l.record_type
,l.line_id
,l.line_object_type
,l.line_object
,m.merchandise_category
,m.sku_id
,m.style_reference_id
,m.class_code
,m.subclass_code
,m.pos_identifier_type
,m.pos_identifier
,m.pos_dept_class
,m.units
,m.salesperson
,m.salesperson2
from `p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_line` as l inner join
`p-asna-analytics-002.edl_landing.ann_ann_aw_transactions_merchdtl` as m on l.if_entry_no =m.if_entry_no and l.line_id =m.line_id  where l.line_object_type =6 ;

entries ae there

for 6,5  the skus are 0


--- query linking 

select 
h.store_no
,cal.week_id as week_id
,count(m.line_id) as Net_Sales_Unit_cnt

from edl_landing.ann_ann_aw_transactions_header h
join edl_landing.ann_ann_aw_transactions_line l
 on h.if_entry_no = l.if_entry_no
 and l.line_object_type= 1
 join edl_landing.ann_ann_aw_transactions_merchdtl m
 on l.if_entry_no = m.if_entry_no
 and l.line_id = m.line_id
join edl_landing.ann_calendar cal
on transaction_Date = cast (day_dt as date)
where h.if_entry_no = 1019718423
--where store_no = 1810  
group by h.store_no
,cal.week_id
join (select sku from
(SELECT sku,row_number() over(partition by sku 
                           order by date_time desc
                        ) as rn FROM edl_landing.ann_ann_sap_product) as a where rn=1 ) as p
on on cast(m.sku_id as string) = p.sku
join
edl_landing

----------------------------------------------------------------


select 
l.if_entry_no
,l.interface_control_flag
,l.record_type
,l.line_id
,l.line_object_type
,l.line_object
,l.line_action
,m.record_type as mrectype
,m.line_id as mlineid
,m.merchandise_category
,m.upc_lookup_division
,m.upc_no
,m.sku_id
,p.article  -- can connect to style through this
from edl_landing.ann_ann_aw_transactions_line l inner join
edl_landing.ann_ann_aw_transactions_merchdtl as m 
on l.if_entry_no = m.if_entry_no
and l.line_id = m.line_id
inner join 
(select sku from
(SELECT sku,row_number() over(partition by sku 
                           order by date_time desc
                        ) as rn FROM `p-asna-analytics-002.edl_landing.ann_ann_sap_product`) as a where rn=1 ) as p
                        on cast(m.sku_id as string) = p.sku ;
					
-------------------------------------------------------------------


					
select a.* from (select *
from (SELECT 
*,
row_number() over(partition by style_number
                           order by date_time desc
                        ) as rn from edl_landing.ann_ann_sap_style) as sty where rn=1 ) as a
left outer join (select  cast(substr(article,1,15) as int64) as article,dept as dept,
class as class from `p-asna-analytics-002.edl_landing.ann_ann_sap_product`
group by
article,
dept,
class) as b
on b.article=cast(a.style_number as int64)



select  cast(substr(article,1,15) as int64) as article
from `p-asna-analytics-002.edl_landing.ann_ann_sap_product` as a left outer join edl_landing.ann_ann_sap_style as b
on cast(b.style_number as int64)=cast(substr(a.article,1,15) as int64) where b.style_number is null;


pre_atg_order_shipment.hql
pre_atg_order_payment.hql
pre_atg_order_header.hql
pre_atg_order_commerceitem.hql
pre_atg_order_relationship.hql
pre_atg_order_commerceitemadjustments.hql
pre_atg_order_commerceitemcoupons.hql

pre_atg_order_return.hql
pre_atg_order_return_method.hql
pre_atg_order_return_commerceitem.hql

edl_hql_to_gcp -t ann_ann_bi_transaction_fpnfp 45

nohup edl_hql_to_gcp -t ann_ann_bi_transaction_fpnfp 3 & 
nohup edl_hql_to_gcp -t ann_ann_sap_exchrate 3 &
nohup edl_hql_to_gcp -t ann_ann_atg_transactions 3 &
nohup edl_hql_to_gcp -t ann_ann_atg_returns 3 &
nohup edl_hql_to_gcp -t ann_ann_aw_transactions_merchdtl 3 &
nohup edl_hql_to_gcp -t ann_ann_aw_transactions_linenotes 3 &
nohup edl_hql_to_gcp -t ann_ann_aw_transactions_line 3 &
nohup edl_hql_to_gcp -t ann_ann_aw_transactions_header 3 &
nohup edl_hql_to_gcp -t ann_ann_aw_transactions_returndtl 3 &
nohup edl_hql_to_gcp -t ann_ann_aw_transactions_taxdetail 3 &
nohup edl_hql_to_gcp -t ann_ann_aw_transactions_discountdtl 3 &
nohup edl_hql_to_gcp -t ann_ann_sap_product 3 &
nohup edl_hql_to_gcp -t ann_ann_sap_style 3 &
nohup edl_hql_to_gcp -t ann_ann_sap_store 3

