select count(*) from  DVZEKE.VMCI019_CUSTOMER -- 101,121,372

--Number of customer which are present in customer table - Desired Number of rows from customer needs to be extracted 


--Optimal way 
select	count(distinct c.id_cust)		
from	DVZEKE.VMCI019_CUSTOMER c 	
join DVZEKE.VMCI105_CHAIN_CUST co  		
	on c.id_cust = co.id_cust		
where	co.id_chain in (4,7)   --47,678,130

--or

select	* 
from	DVZEKE.VMCI019_CUSTOMER c 
where	c.id_cust in (
	select	distinct co.id_cust 
	from	DVZEKE.VMCI105_CHAIN_CUST co 
	where	co.id_chain in (4,7))  --47,678,130
	


--Number of customer which are there in chain_cust tsble 
select count(*) from (
select co.id_cust,co.id_chain from
DVZEKE.VMCI105_CHAIN_CUST co  	
where co.id_chain in (4,7)
group by co.id_cust,co.id_chain) as a --- 56,529,679		

--Q : which one will there be in the customer table ? As I think there is a single table for both LB and CA .

--customer which are there both in CA and LB
select count(*) as cnt from (
select co.id_cust from
DVZEKE.VMCI105_CHAIN_CUST co  
where co.id_chain in (4,7)
group by co.id_cust
having count(distinct co.id_chain)>1
) as tot;  --8,242,353

--This particular cust_id is there in both chain id.

select * from  DVZEKE.VMCI105_CHAIN_CUST where id_cust= '31501052'
select * from  DVZEKE.VMCI019_CUSTOMER where id_cust= '31501052'

-------------------------------------------------------------------------


select count(distinct ea.id_email)	
from DMMCIPI04.VMCI173_EMAIL_ADDRESS  ea		
join DMMCIPI04.VMCI172_CHAIN_CUST_EMAIL cce		
on ea.id_email = cce.id_email		
where cce.id_chain in (4,7)		--23,671,529

select	count(*) 
from	DMMCIPI04.VMCI173_EMAIL_ADDRESS ea 
where	ea.id_email in (
	select	distinct cce.id_email 
	from	DMMCIPI04.VMCI172_CHAIN_CUST_EMAIL cce 
	where	cce.id_chain in (4,7))  --23,671,529
	
	
select	count(*) 
from	DMMCIPI04.VMCI173_FULL_EMAIL_ADDRESS fea 
where	fea.id_email in (
	select	distinct cce.id_email 
	from	DMMCIPI04.VMCI172_CHAIN_CUST_EMAIL cce 
	where	cce.id_chain in (4,7)) --23,671,529

select	count(*) 
from	DMMCIPI04.VMCI173_FULL_EMAIL_ADDR_STATUS feas 
where	feas.id_email in (
	select	distinct cce.id_email 
	from	DMMCIPI04.VMCI172_CHAIN_CUST_EMAIL cce 
	where	cce.id_chain in (4,7)) --23,671,529
	
	
-----------------------------------------------------------------------------

select	id_chain,count(*) 
from	DVZEKE.VMCI079_CLUB_MEMB 
where	id_chain in (4,7) 
group by id_chain  

ID_CHAIN	Count(*)
4	      2,803,410
 
 
select	count(*) 
from	DMMCIPI04.VMCI079_CLUB_MEMB cm 
where	cm.id_cust in (
	select	distinct co.id_cust 
	from	DVZEKE.VMCI105_CHAIN_CUST co 
	where	co.id_chain in (4,7))  


select
sum(case when a.id_club_memb is null then 0 else 1 end) as match_count
,sum(case when a.id_club_memb is null then 1 else 0 end) as orphan_count
from DVZEKE.VMCI079_CLUB_MEMB as a
join DVZEKE.VMCI005_CSTID_XREF b
on a.id_club_memb=b.id_alt_cust  and a.id_chain = 4;


------------------------------------------------------------------------------

--Club members in CA 
select	id_chain,count(*) 
from	DMMCIPI04.VMCI079_CLUB_MEMB 
where	id_chain in (4,7) 
group by id_chain  

--4	2,766,073


--Club member id which belongs to CA
select	count(*) 
from	DMMCIPI04.VMCI079_CLUB_MEMB c 
where	c.id_cust in (
	select	distinct co.id_cust 
	from	DVZEKE.VMCI105_CHAIN_CUST co 
	where	co.id_chain in (4,7))
and c.id_chain in (4,7) --2,766,054

--These 19 customers have membership with CA , but the customer id is not present in Chain Customer table.
select	count(*) 
from	DMMCIPI04.VMCI079_CLUB_MEMB c 
where	c.id_cust not in (
	select	distinct co.id_cust 
	from	DVZEKE.VMCI105_CHAIN_CUST co 
	where	co.id_chain in (4,7))
and c.id_chain in (4,7) --19


--These are some customers which belong to CA and LB but does not have membership with LB or CA 
select	count(*) 
from	DMMCIPI04.VMCI079_CLUB_MEMB c 
where	c.id_cust in (
	select	distinct co.id_cust 
	from	DVZEKE.VMCI105_CHAIN_CUST co 
	where	co.id_chain in (4,7))
	and c.id_chain not in (4,7) --93
	


	
	
	
	
	
	
	

	
	