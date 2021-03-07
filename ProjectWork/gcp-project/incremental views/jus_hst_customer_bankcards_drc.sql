CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_customer_bankcards_drc
as SELECT TBL.*
from( SELECT d.*
             ,row_number() OVER (PARTITION BY bankcard_acct_token
                                             ,individual_id
                                             ,account_number 
                                             order by BATCH_ID desc) as rn 
      FROM ascena_staging_dev.jus_hst_customer_bankcards_dr d
    ) as TBL
where   rn=1
;