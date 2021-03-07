set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.transaction_notes (
transaction_key	char(32)
,transaction_notes_line_num	int
,transaction_notes_sequence_num	int
,note_type_desc	string
,free_form_notes_desc	string
,edl_create_tms	timestamp
,edl_create_job_nam	string
,edl_last_update_tms	timestamp
,edl_last_update_job_nam	string
,brand_cd	string           
 )
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


insert overwrite table ascena_analytic_mart.transaction_notes
select  transaction_key
,transaction_notes_line_num
,transaction_notes_sequence_num
,note_type_desc
,free_form_notes_desc
,edl_create_tms
,edl_create_job_nam
,edl_last_update_tms
,edl_last_update_job_nam
,brand_cd             
from  ascena_conform.transaction_notes
where edl_last_update_tms > date_add(current_date,-3)
;