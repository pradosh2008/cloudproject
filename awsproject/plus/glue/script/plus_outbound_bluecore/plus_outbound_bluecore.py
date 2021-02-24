import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import current_date,DataFrame
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StructType,StructField,StringType,LongType,IntegerType
from pyspark.sql.functions import month,year ,concat_ws, md5, current_date
from pyspark.sql.types import *
from foundationutil import *
import boto3
import re
import datetime

def table_exists(db_name,search_tbl_name,spark_ssn):
    table_collection = spark_ssn.catalog.listTables(db_name)
    table_names = [table.name for table in table_collection]
    print(table_names)
    exists_ind = search_tbl_name in table_names
    return exists_ind

def get_eprefix():
    accountId = boto3.client('sts').get_caller_identity().get('Account')
    print("accountId: ",accountId)
    if accountId == '512757696531':
        _prfx = 'p'
    elif accountId == '951523344214':
        _prfx = 'qa'
    else:
        _prfx = 'd'
    return _prfx

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
rundate = datetime.datetime.now().strftime("%Y%m%d")
print("rundate: ",rundate)
eprfx = get_eprefix()

target_db = "aadp_model_scores"
history_tbl = "bluecore_extract_history"
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
query = 'use {}'.format(target_db)
spark.sql(query)

landing_bucket_name = 's3-'+eprfx+'-ascena-aadp-landing'

if eprfx == 'd':
    mart_bucket_name = 's3-'+eprfx+'-ascena-d2-asc-aadp-mart'
else:
    mart_bucket_name = 's3-'+eprfx+'-ascena-aadp-mart'
    

outbound_bucket_name = 's3-'+eprfx+'-ascena-caa-outbound'


if eprfx == 'd':
   arc_bucket_name = 'd2-asc-aadp-arc'
else:
    arc_bucket_name = eprfx+'-ascena-aadp-arc'



source_prefix='//ds-model-output//plus'
source_ccps='crosschannel_propensity_storetoecomm'
source_churn='churn'

ss3 = boto3.resource('s3')

src = ss3.Bucket(outbound_bucket_name)
print(src)
dst = ss3.Bucket(arc_bucket_name)
print(dst)

#src = ss3.Bucket('s3-d-ascena-caa-outbound')
#dst = ss3.Bucket('d2-asc-aadp-arc')


#for obj in src.objects.filter(Prefix='bluecore'):
#            copy_source = { 'Bucket': outbound_bucket_name,
#                           'Key': obj.key}
#            print(obj.key)
#            print(copy_source)
#            arc_key = 'bluecore' + obj.key[len('bluecore'):]
#            print("arc_key: ",arc_key)
#
#new_obj = dst.Object(arc_key)
#new_obj.copy(copy_source)
#
#if len(list(filter(None,arc_key.split('/')))) ==2:
#    del_object = src.Object(arc_key)
#    print(del_object)
#    response = del_object.delete()

schema_model_metadata = StructType([\
    StructField("model_id", IntegerType(), True),\
    StructField("model_name", StringType(), True),\
    StructField("model_desc", StringType(), True),\
    StructField("create_ts", TimestampType(), True)])

metadata_df = spark.read\
.option("header", "false")\
.option("quote", "\"")\
.option("ignoreLeadingWhiteSpace", "true")\
.schema(schema_model_metadata)\
.csv("s3://"+landing_bucket_name+"/ds-model-output/plus/model_metadata/")


metadata_df.createOrReplaceTempView('model_metadata')


##Model Churn dataframe
schema_churn = StructType([\
    StructField("brand_customer_id", StringType(), True),\
    StructField("score", DecimalType(12,10), True),\
    StructField("model_id", IntegerType(), True),\
    StructField("timestamp", TimestampType(), True),\
    StructField("workflow_uid", StringType(), True)])


model1_df = spark.read\
.option("header", "false")\
.option("quote", "\"")\
.option("ignoreLeadingWhiteSpace", "true")\
.schema(schema_churn)\
.csv("s3://"+landing_bucket_name+"/ds-model-output/plus/churn/")

model1_df.createOrReplaceTempView('churn')

##Model ccps dataframe
schema_ccps = StructType([\
    StructField("brand_customer_id", StringType(), True),\
    StructField("score", DecimalType(12,10), True),\
    StructField("model_id", IntegerType(), True),\
    StructField("timestamp", TimestampType(), True),\
    StructField("workflow_uid", StringType(), True)])


model2_df = spark.read\
.option("header", "false")\
.option("quote", "\"")\
.option("ignoreLeadingWhiteSpace", "true")\
.schema(schema_ccps)\
.csv("s3://"+landing_bucket_name+"/ds-model-output/plus/crosschannel_propensity_storetoecomm/")

model2_df.createOrReplaceTempView('crosschannel_propensity_storetoecomm')


##Reading brand_customer_email table
brand_customer_email_df=spark.read.parquet("s3://"+ mart_bucket_name+"/plus/plus_brand_customer_email/")
brand_customer_email_df.createOrReplaceTempView('brand_customer_email')



customer_model_score_df = glueContext.sql("""Select 
          e.brand_customer_key
        , e.brand_customer_email_key
        , e.email_key
        , mc.brand_customer_id as customer_id
        , e.full_email_addr as customer_email
        , mm.model_id
        , mm.model_name as model_name
        , round(mc.score,6) as model_score
        , mc.timestamp as create_ts
from (select cast(brand_customer_id as integer) as brand_customer_id
                  , model_id
                  , score
                  , timestamp 
                  , max(timestamp) over ( partition by brand_customer_id
                                                    , model_id
                      ) as max_timestamp
             from churn
) mc
join model_metadata mm
   on mc.model_id = mm.model_id
join brand_customer_email e
   on mc.brand_customer_id = e.brand_customer_id
  and e.brand_cd='LB'
  and e.email_domain_addr not in ('orders.fiftyone.com','keynote.com')
where mc.max_timestamp = mc.timestamp 
UNION
Select e.brand_customer_key
    , e.brand_customer_email_key
    , e.email_key
    , ccpsr.brand_customer_id as customer_id
    , e.full_email_addr as customer_email
    , mm.model_id
    , mm.model_name as model_name
    , round(ccpsr.score,6) as model_score
    , ccpsr.timestamp as create_ts
from (select cast(brand_customer_id as integer) as brand_customer_id
                    , model_id
                    , score
                    , timestamp
                    , max(timestamp) over ( partition by brand_customer_id
                                                    , model_id
                      ) as max_timestamp
      from crosschannel_propensity_storetoecomm
) ccpsr
join model_metadata mm
   on ccpsr.model_id = mm.model_id
join brand_customer_email e
   on ccpsr.brand_customer_id = e.brand_customer_id
  and e.brand_cd='LB'
  and e.email_domain_addr not in ('orders.fiftyone.com','keynote.com')
where ccpsr.max_timestamp = ccpsr.timestamp 
""")
customer_model_score_df.createOrReplaceTempView('customer_model_score')



bluecore_extract_df = glueContext.sql("""Select 
         cms.brand_customer_key
        ,cms.brand_customer_email_key
        ,cms.email_key
        ,cms.customer_id
        ,cms.customer_email
        ,max(case when cms.model_id = 1
                   then cms.model_score
             end)                               as churn_score
        ,max(case when cms.model_id = 1
                then cms.create_ts 
          end)                                  as churn_create_tms
        ,max(case when cms.model_id = 2
                then model_score 
          end)                                  as ccps_score
        ,max(case when cms.model_id = 2
                then cms.create_ts 
          end)                                  as ccps_create_tms
  from customer_model_score cms
  group by cms.brand_customer_key
        ,cms.brand_customer_email_key
        ,cms.email_key
        ,cms.customer_id
        ,cms.customer_email
 """)
map_keyval = {'row_hashed_val':['customer_email','customer_id','churn_score','churn_create_tms','ccps_score','ccps_create_tms']
           }
audit_cols=['foundation_program_nam','foundation_insert_tms']
bluecore_extract_hashkey_df = hash_key(bluecore_extract_df,map_keyval)
bluecore_extract_audit_df = add_auditcol(bluecore_extract_hashkey_df.withColumn("run_dt",lit(rundate))
                                            ,*audit_cols
                                            ,job_name=args['JOB_NAME']
                                          )
#bluecore_extract_audit_df.show()
history_table_exists = table_exists(target_db,history_tbl,spark)
print("history table exists: ",history_table_exists)
if history_table_exists:
    print(history_tbl+" table exists")
    bluecore_extract_hist_df=spark.read.parquet('s3://'+ mart_bucket_name+'/plus/'+history_tbl+'/')
    inc_bluecore_extract_df=inc_leftjoin_fdn(bluecore_extract_hist_df,bluecore_extract_audit_df)
else:
    print(history_tbl+" table does not exist")
    inc_bluecore_extract_df=bluecore_extract_audit_df 

if inc_bluecore_extract_df.rdd.isEmpty():
    print("Zero Records in the inc bluecore extract df")
else:
    inc_bluecore_extract_df.write.saveAsTable(history_tbl, format='parquet',mode='append'
                            ,path='s3://'+mart_bucket_name+'/plus/'+history_tbl)


    extract_sql="Select distinct customer_email "\
            +",customer_id "\
            +",churn_score "\
            +",churn_create_tms "\
            +",ccps_score "\
            +",ccps_create_tms "\
        +"from bluecore_extract_history "\
        +"where customer_email is not null"\
        +"   or trim(customer_email) <> ''"\
        +"and run_dt = '"+rundate+"'"

    print(extract_sql)
    bluecore_file_df = glueContext.sql(extract_sql)

    bluecore_file_df.coalesce(1).write.mode("append").option("header","true").csv("s3://"+outbound_bucket_name+"/temp")
    bluecore_file_df.coalesce(1).write.mode("append").option("header","true").csv("s3://"+arc_bucket_name+"/bluecore")
    ##


    out = ss3.Bucket(outbound_bucket_name)
    for obj in out.objects.filter(Prefix="temp"):
        out_key = obj.key
        print(out_key)

    renamed_key = "bluecore/bc_customer_model_score_" + rundate + ".csv"
    print(renamed_key)
    ss3.Object(outbound_bucket_name, renamed_key).copy_from(CopySource=outbound_bucket_name+"/"+out_key)
    ss3.Object(outbound_bucket_name, out_key).delete()
    status=True

    move_to_archive(status,source_prefix,source_churn)

    move_to_archive(status,source_prefix,source_ccps)

