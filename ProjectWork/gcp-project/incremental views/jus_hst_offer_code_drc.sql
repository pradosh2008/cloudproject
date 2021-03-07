CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_offer_codes_drc
AS
SELECT TBL.*
from ( SELECT d.*
             , row_number() over ( partition by OFFER_CODE_ID,OFFER_CODE
                                   order by BATCH_ID desc
                          ) as rn
       from ascena_staging_dev.jus_hst_offer_codes_dr d
     ) as TBL
where rn=1
;