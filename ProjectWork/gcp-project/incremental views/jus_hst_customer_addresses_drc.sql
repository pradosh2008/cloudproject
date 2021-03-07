CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_customer_addresses_drc
AS
SELECT TBL.*
from ( SELECT d.*
             , row_number() over ( partition by INDIVIDUAL_ID,ADDRESS_1,ADDRESS_2,CITY,STATE_PROVINCE,ZIP_POSTAL_CODE,ZIP_4
                                   order by BATCH_ID desc
                          ) as rn
       from ascena_staging_dev.jus_hst_customer_addresses_dr d
     ) as TBL
where rn=1
;