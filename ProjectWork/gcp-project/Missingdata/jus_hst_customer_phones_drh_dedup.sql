CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_customer_phones_drh_dedup
AS
SELECT TBL.*
from ( SELECT d.*
             , row_number() over ( partition by individual_id
                                               ,phone_no
                                               order by BATCH_ID desc
                          ) as rn
       from ascena_staging_dev.jus_hst_customer_phones_drh d
     ) as TBL
where rn=1
;