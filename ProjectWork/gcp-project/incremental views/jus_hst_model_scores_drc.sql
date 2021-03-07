CREATE OR REPLACE VIEW ASCENA_STAGING_DEV.jus_hst_model_scores_drc
AS
SELECT TBL.*
from ( SELECT d.*
             , row_number() over ( partition by model_score_run_id
                                               ,original_individual_id
                                               order by BATCH_ID desc
                          ) as rn
       from ascena_staging_dev.jus_hst_model_scores_dr d
     ) as TBL
where rn=1
;