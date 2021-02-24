import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#from foundationutil_updated import *

#This python script will be stored in the lib folder . The s3 path will be passed to the glue job.
# import additional libraries  
import pyspark,boto3
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import month,year ,concat_ws, md5 , current_timestamp ,lit,col ,to_timestamp,concat,format_string,trim,sha2,split,input_file_name,element_at
from awsglue.dynamicframe import DynamicFrame
import json
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import desc


#initialize target parent bucket name and landing schea
target_bucket_name="s3-p-ascena-aadp-mart"  
foundation_bucket_name="s3-p-ascena-aadp-foundation"

#initialize glue client
glue_client = boto3.client('glue', region_name = 'us-east-1')

target_db="plus_mart"
#initialize role
gluerole='p-CAA-Service-Glue-iam-role'


def register_fdn_view(foundation_prefix,foundation_tbl_list,spark):
    for foundation_tbl in foundation_tbl_list:
        foundation_path="s3://"+foundation_bucket_name+foundation_prefix+"/"+foundation_tbl
        fdn_hash_df=spark.read.parquet(foundation_path)
        fdn_hash_df.createOrReplaceTempView(foundation_tbl)
        print('table has been created for '+foundation_tbl)
        
def mart_calc_rowhash(mart_df):
    cols = [c for c in mart_df.columns if c.lower() not in {'extract_ts','batch_id','mart_job_nam','mart_insert_tms','batch_id'}]
    #source_clean_df.withColumn("row_sha2_val", concat_ws("||", *cols)).show(truncate=False)
    #source_clean_df.withColumn("row_sha2", sha2(concat_ws("||", *cols), 256)).show(truncate=False)
    mart_hash_df=mart_df.withColumn("row_hashed_val", sha2(concat_ws("||", *cols), 256))
    return mart_hash_df

def write_to_sink(target_sdf,target_tbl,partitionKeys,glueContext,target_prefix):
    target_gdf = DynamicFrame.fromDF(target_sdf, glueContext, "target_sdf")
    logger = glueContext.get_logger()
    target_path="s3://"+target_bucket_name+target_prefix+"/"+target_tbl
    logger.info('write_to_sink path is {}'.format(target_path))
    logger.info('write_to_sink getSink()')
    sink = glueContext.getSink(connection_type="s3", path=target_path,\
              enableUpdateCatalog=True,\
              updateBehavior="UPDATE_IN_DATABASE",\
              partitionKeys=partitionKeys\
            )
    logger.info('write_to_sink setting catalog  info')
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=target_db, catalogTableName=target_tbl)
    sink.writeFrame(target_gdf)
    status=True
    return status

#to read mart , define all this variable
def read_mart(target_prefix,target_tbl,spark):
    #target_path="s3://d1-asc-aadp-work/dim_customer_test"
    target_path="s3://"+target_bucket_name+target_prefix+"/"+target_tbl
    #source_df=spark.read.option("delimiter","|").option("header", "true").csv(target_path+'//*')
    mart_df=spark.read.parquet(target_path)
    return mart_df

def inc_leftjoin_mart(mart_df,temp_df):
    inc_leftjoined_mart_df = temp_df.alias("a")\
    .join(mart_df.alias("b"), col("b.row_hashed_val") == col("a.row_hashed_val"), how='left')\
    .filter(col("b.row_hashed_val").isNull())\
    .select([col("a."+xx) for xx in temp_df.alias("a").columns])
    #.show()
    #print(inc_leftjoined_fdn_df.count())
    #inc_leftjoined_fdn_df.show(truncate=False)
    return inc_leftjoined_mart_df
def load_refresh_mart_tbl(mart_df,target_tbl,target_prefix,*partitionKeys):

    crawler_lists = glue_client.list_crawlers(MaxResults=1000)
    available_crawlers = crawler_lists["CrawlerNames"]
    crawler_name=target_tbl+'_crawler'
    
    # windowSpec  = Window.partitionBy(p_key).orderBy(ord_col)

    # mart_stg_df_latest = mart_stg_df.withColumn("row_num",row_number().over(windowSpec)).sort(desc(ord_col))
    # mart_df = mart_stg_df_latest.filter("row_num=1")
    print('mart data frame count: {}'.format(mart_df.count()))
    print('number of partitions: {}'.format(mart_df.rdd.getNumPartitions()))

    mart_df.write.partitionBy(*partitionKeys)\
    .format('parquet').mode("overwrite")\
    .save('s3://'+target_bucket_name+target_prefix+"/"+target_tbl)
    print("Data OverWrite to target : Done \n")
    print(crawler_name + " needs to be run")
    if crawler_name not in available_crawlers :
    #Attempt to create and start a glue crawler on PSV table or update and start it if it already exists.
        print("creating and running the crawler")
        glue_client.create_crawler(
            Name = crawler_name,
            Role = gluerole,
            DatabaseName = target_db,
            Targets = 
            {
                'S3Targets': 
                [
                    {
                        'Path':'s3://'+target_bucket_name+target_prefix+"/"+target_tbl
                    }
                ]
            }
        )
        glue_client.start_crawler(
            Name = crawler_name
        )
    else:
        print("running the exisiting crawler")
        glue_client.start_crawler(
            Name = crawler_name
        )
