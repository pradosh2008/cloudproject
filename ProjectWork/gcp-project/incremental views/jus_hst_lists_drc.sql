CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_lists_drc
as SELECT TBL.*
from( SELECT d.*
             ,row_number() OVER (PARTITION BY list_id 
                                             order by BATCH_ID desc) as rn 
      FROM ascena_staging_dev.jus_hst_lists_dr d
    ) as TBL
where   rn=1
;