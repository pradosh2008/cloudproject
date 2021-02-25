from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import current_date,DataFrame
# from awsglue.dynamicframe import DynamicFrame
# from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StructType,StructField,StringType,LongType,IntegerType
import boto3
import re
from pyspark.sql.functions import month,year ,concat_ws, md5, current_date
from pyspark.sql.types import *

import datetime
currentdate = datetime.datetime.now().strftime("%Y%m%d")
print(currentdate)

# import boto3
# s3_client = boto3.client('s3')
# s3_resource = boto3.resource('s3')
# def rename_targetfile():
#     source_bucket_name = "d2-asc-aadp-foundation"
#     src = s3_resource.Bucket(source_bucket_name)
#     prefix = "plus/outbound/bluecore_extract/temp"
#     for obj in src.objects.filter(Prefix=prefix):
#         source_key = obj.key
#         print(source_key)
#
#     renamed_key = "plus/outbound/bluecore_extract/dest/bc_customer_model_score_" + currentdate + ".csv"
#     print(renamed_key)
#     s3_resource.Object(source_bucket_name, renamed_key).copy_from(CopySource=source_bucket_name+"/"+source_key)
#     s3_resource.Object(source_bucket_name, source_key).delete()
# rename_targetfile()

target_db = "aadp_model_scores"
target_tbl = "bluecore_extract_history"
query = 'use {}'.format(target_db)
spark.sql(query)

tables_collection = spark.catalog.listTables(target_db)
table_names_in_db = [table.name for table in tables_collection]

table_exists = target_tbl in table_names_in_db
print(table_exists)