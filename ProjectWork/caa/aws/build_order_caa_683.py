from foundationutil import *

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "aadp-glue", table_name = "olist_orders", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "aadp-glue", table_name = "olist_orders", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("order_id", "string", "order_id", "string"), ("customer_id", "string", "customer_id", "string"), ("order_status", "string", "order_status", "string"), ("order_purchase_timestamp", "string", "order_purchase_timestamp", "string"), ("order_approved_at", "string", "order_approved_at", "string"), ("order_delivered_carrier_date", "string", "order_delivered_carrier_date", "string"), ("order_delivered_customer_date", "string", "order_delivered_customer_date", "string"), ("order_estimated_delivery_date", "string", "order_estimated_delivery_date", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("order_id", "string", "order_id", "string"), ("customer_id", "string", "customer_id", "string"), ("order_status", "string", "order_status", "string"), ("order_purchase_timestamp", "string", "order_purchase_timestamp", "string"), ("order_approved_at", "string", "order_approved_at", "string"), ("order_delivered_carrier_date", "string", "order_delivered_carrier_date", "string"), ("order_delivered_customer_date", "string", "order_delivered_customer_date", "string"), ("order_estimated_delivery_date", "string", "order_estimated_delivery_date", "string")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://asc-aadpdatalakelayer"}, format = "json", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]


# job name
job_name=args['JOB_NAME']

# specify source table name  and target table name
source_tbl='olist_orders'
target_tbl='order_caa_683'

#Specify the data will get load loaded in which folder in target bucket
#source_prefix='//plus//edw'
source_prefix=''

# specify primary key and foreign key columns name
map_keyval = {'olist_orders_test_key':['order_id'],
           'customer_order_key':['order_id','customer_id'],
           'order_delivered_key':['order_id','order_delivered_customer_date']}
           
           
#If the table needs to be partitioned , specify the partition key columns
partition_col="order_purchase_timestamp"
#partitionKeys=['chain_id']
partitionKeys=['transaction_month']

base_gdf=create_base_frame(applymapping1
                            ,map_keyval
                            ,glueContext
                            ,spark
                            ,job_name
                            ,source_tbl
                            ,audit_cols=['foundation_program_nam','foundation_ins_dt'])

target_gdf=create_ym_partitoned_frame(base_gdf,partition_col,glueContext)

write_partitioned_sink(target_gdf,target_tbl,partitionKeys,glueContext,source_prefix)


#datasink2 = glueContext.write_dynamic_frame.from_options(frame = sqlGF, connection_type = "s3", connection_options = {"path": "s3://asc-aadpdatalakelayer/order_caa_683"}, format = "parquet", transformation_ctx = "datasink2")
job.commit()