select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.brand_customer_transaction_xref
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.brand_customer_transaction_xref
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.brand_customer
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.brand_customer
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.brand_customer_email
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.brand_customer_email
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.brand_customer_phone
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.brand_customer_phone
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.brand_customer_membership
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.brand_customer_membership
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.brand_customer_account
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.brand_customer_account
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.brand_customer_addr
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.brand_customer_addr
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.store
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.store
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.item
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.item
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.brand_customer_location
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.brand_customer_location
) as a
;


select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.transaction
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.transaction
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.transaction_detail
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.transaction_detail
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.transaction_tender
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.transaction_tender
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.transaction_discount
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.transaction_discount
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.transaction_fee
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.transaction_fee
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.transaction_notes
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.transaction_notes
) as a
;

select case when sum(c1)-sum(c2)==0 then 'matched counts' else 'mismatched counts' end as result
from
(
select count(*) as c1,0 as c2 from ascena_conform.transaction_tax
union all
select 0 as c1,count(*) as c2 from ascena_analytic_mart.transaction_tax
) as a
;