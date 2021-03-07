create or replace table work.plus_brand_customer_email as
select
TO_HEX(MD5(CONCAT(CAST(case when cce.id_chain = 7 then 'LB' when cce.id_chain = 4 then 'CA' else NULL end as string)
                  ,'|',cast(cce.id_cust as string)))) 
as brand_customer_key
,cast(cce.id_email as string) as email_id	
,cast(cce.id_cust as string) as brand_customer_id
,cce.cd_dbl_opt_in as dbl_opt_in_cd
,cce.cd_email as email_cd
,cce.da_em_cust_verified as em_customer_verified_dt
,cce.da_email_code as email_code_dt
,cce.da_email_created as email_created_dt
,cce.da_email_updated as email_updated_dt
,case when trim(cce.fl_habeas_compl)='Y' then 1
      when trim(cce.fl_habeas_compl)='N' then 0
      else NULL end as habeas_compl_ind
,case when trim(cce.fl_ok_to_email) in ('Y','U') then 1
      when trim(cce.fl_ok_to_email)='N' then 0
      else NULL end as ok_to_email_ind
/*from  chain cust full email table */
,ccf.id_domain as domain_id
,ccf.cd_email_style as email_style_cd
,case when trim(ccf.fl_primary_email)='Y' then 1
      when trim(ccf.fl_primary_email)='N' then 0
      else NULL end as primary_email_ind
,case when trim(ccf.fl_email_valid)='Y' then 1
      when trim(ccf.fl_email_valid)='N' then 0
      else NULL end as email_valid_ind 
,fea.id_email_domain as email_domain_id
,fea.nu_bb_soft as bb_soft_num
,fea.nu_bb_hard as bb_hard_num
,fea.da_tmci173_created as tmci173_created_dt
,fea.da_tmci173_updated as tmci173_updated_dt
,fea.in_cust_source_first as customer_source_first_num
,fea.in_cust_source_2nd_first as customer_source_second_first_num
,fea.in_cust_source_last as customer_source_last_num
,fea.in_cust_source_2nd_last as customer_source_second_last_num
,fea.cd_email_invalid as email_invalid_cd
,fea.da_frst_hard_bb as first_hard_bb_dt
,fea.da_last_hard_bb as last_hard_bb_dt
,fea.da_first_soft_bb as first_soft_bb_dt
,fea.da_last_soft_bb as last_soft_bb_dt

,lco.ts_last_click as last_click_tms                
,lco.ts_last_open as last_open_tms                  
,lco.da_last_click as last_click_dt                 
,lco.da_last_open as last_open_dt                 

,CURRENT_TIMESTAMP AS edl_create_tms
,'CRMCUST' AS edl_create_job_nam
,CURRENT_TIMESTAMP AS edl_last_update_tms
,'CRMCUST' AS edl_last_update_job_nam
,cast(case when cce.id_chain = 7 then 'LB' when cce.id_chain = 4 then 'CA' else NULL end as string) as brand_cd
from edl_stage.plus_vmci172_chain_cust_email as cce
/* This part is not needed as we are already having this condition while extracting*/
-- LEFT join                               				
        -- ( select distinct id_cust from edl_stage.plus_vmci105_chain_cust
                -- where id_chain in (4,7))cc
-- on cc.id_cust = cce.id_cust 
/*Join with chain cust full email table */
inner join edl_stage.plus_vmci972_chain_cust_fullemail as ccf
on cce.id_chain=ccf.id_chain
and cce.id_cust=ccf.id_cust
and cce.id_email=ccf.id_email 
/* join with full_email_address table */
left outer join edl_stage.plus_vmci173_full_email_address as fea
on cce.id_email=fea.id_email
left outer join edl_stage.plus_vmci995_esp_last_click_open as lco
on cce.id_chain=lco.id_chain
and cce.id_email=lco.id_email 
where cce.id_chain in (4,7) --This condition is vital