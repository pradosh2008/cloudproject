Introduction to AWS and cloud   
AWS architecture   https://aws.amazon.com/architecture/?solutions-all.sort-by=item.additionalFields.sortDate&solutions-all.sort-order=desc&whitepapers-main.sort-by=item.additionalFields.sortDate&whitepapers-main.sort-order=desc&reference-architecture.sort-by=item.additionalFields.sortDate&reference-architecture.sort-order=desc


bq cp --force analytic_mart_dev.pre_store_inventory analytic_mart.pre_store_inventory


bq cp --force edl_stage.pre_atg_order_commerceItem edl_archive.pre_atg_order_commerceItem_03122019
bq cp --force edl_stage.pre_atg_order_header edl_archive.pre_atg_order_header_03122019
bq cp --force edl_stage.pre_atg_order_commerceitemadjustments edl_archive.pre_atg_order_commerceitemadjustments_03122019
bq cp --force edl_stage.pre_atg_order_commerceitemcoupons edl_archive.pre_atg_order_commerceitemcoupons_03122019
bq cp --force edl_stage.pre_atg_order_shipment edl_archive.pre_atg_order_shipment_03122019
bq cp --force edl_stage.pre_atg_order_payment edl_archive.pre_atg_order_payment_03122019
bq cp --force edl_stage.pre_atg_order_shipment_tracking edl_archive.pre_atg_order_shipment_tracking_03122019



-- SELECT
--   style_id
-- FROM (
--   SELECT
--     *,
--     ROW_NUMBER() OVER(PARTITION BY style_id 
--                       ORDER BY batch_id DESC) AS rn
--   FROM
--     edl_stage.pre_sap_prod_hier ) AS a
-- WHERE
--   rn=1


bq load --replace=true --source_format=CSV --field_delimiter="|" --skip_leading_rows=1 \
        --schema=/home/gcpinteg/schema/edl_stage/planschema.json edl_landing.ann_plan_data \
        'gs://p-asna-datasink-003/pre/sap/ANN_ACC_EDL_PLAN_*.txt'


gsutil mv gs://p-asna-datasink-003/pre/sap/ANN_ACC_EDL_PLAN_*.txt  gs://p-asna-datasink-003/pre/archive

ANN_ACC_EDL_PLAN_20191117155020.txt
ANN_ACC_EDL_PLAN_20191124135609.txt
ANN_ACC_EDL_PLAN_20191201141739.txt


-- select class_id, department_id, division_id, chain_id, channel_id, country_id, week_id, month_id,sales_type_cd,count(*) from `edl_stage.pre_sap_plan_data` group by class_id, department_id, division_id, chain_id, channel_id, country_id, week_id, month_id,sales_type_cd having count(*)>1;


-- select * from `edl_stage.pre_sap_plan_data` where 
-- class_id='L34375008'  and  department_id=375 and  division_id='L34' and  chain_id=64 and  channel_id=5 and  country_id='US' and  week_id=201951 and  month_id=2019012 and sales_type_cd='F'
