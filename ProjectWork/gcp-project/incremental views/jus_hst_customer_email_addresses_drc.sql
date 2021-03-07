CREATE 
    OR 
REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_customer_email_addresses_drc
AS
WITH MID AS
(
    select  TBL.individual_id
     ,TBL.email_address
     ,TBL.MAX_BATCH_ID 
    from
    (
        SELECT
         individual_id as individual_id
        ,email_address as email_address
        ,BATCH_ID AS MAX_BATCH_ID
        ,row_number() over(
        partition by individual_id, email_address 
                                   order by BATCH_ID desc
                                ) as rn
            from    ascena_staging_dev.jus_hst_customer_email_addresses_dr
    ) as TBL
    where   rn=1)
SELECT  TBL1.* 
FROM    ASCENA_STAGING_DEV.jus_hst_customer_email_addresses_dr TBL1,
        MID
where   TBL1.BATCH_ID = MID.MAX_BATCH_ID
    AND TBL1.individual_id = MID.individual_id
    AND TBL1.email_address = MID.email_address ;