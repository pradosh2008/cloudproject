--primary key check
{code}
select
email_id,
brand_customer_key 
from work.plus_brand_customer_email
group by 1,2
having count(*)>1
{code}
--passed

--alternate column key check for any non CA non LB records
{code}
select
email_id,
brand_customer_id,
brand_cd 
from work.plus_brand_customer_email
group by 1,2,3
having count(*)>1
{code}
--passed

--null check 
{code}
SELECT COUNT(*) cnt FROM
(SELECT * 
FROM work.plus_brand_customer_email
WHERE email_id is NULL or brand_customer_key IS NULL
)
{code}
--passed

--brand_cd null check for any non CA non LB records
{code}
SELECT COUNT(*) cnt FROM
(SELECT * 
FROM work.plus_brand_customer_email
WHERE brand_cd is NULL
)
{code}
--passed

--completeness check
inner join because plus_vmci172_chain_cust_email is full load and plus_vmci972_chain_cust_fullemail is incremental load and currently it does nothave history also.
{code}
SELECT COUNT(*) cnt FROM (
SELECT bce.*
FROM `work.plus_brand_customer_email` bce
LEFT OUTER JOIN (SELECT 
cast(cce.id_email as string) as email_id		  
,TO_HEX(MD5(CONCAT(CAST(case when cce.id_chain = 7 then 'LB' else 'CA' end as string)
                  ,'|',cast(cce.id_cust as string)))) 
as brand_customer_key
from edl_stage_qa.plus_vmci172_chain_cust_email as cce
inner join edl_stage_qa.plus_vmci972_chain_cust_fullemail as ccf
on cce.id_chain=ccf.id_chain
and cce.id_cust=ccf.id_cust
and cce.id_email=ccf.id_email 
) stg
ON bce.brand_customer_key = stg.brand_customer_key and bce.email_id = stg.email_id
WHERE stg.brand_customer_key is null
)
{code}
