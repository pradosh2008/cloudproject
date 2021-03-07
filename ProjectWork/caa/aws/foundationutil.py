#This python script will be stored in the lib folder . The s3 path will be passed to the glue job.
# import additional libraries  
import pyspark,boto3
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import month,year ,concat_ws, md5 , current_timestamp ,lit,col ,to_timestamp,concat,format_string
from awsglue.dynamicframe import DynamicFrame
import json

#initialize target parent bucket name and landing schea
landing_parent_bucket="s3://asc-aadpdatalakelayer"
schema_bucket_name='asc-aadpschema'

schema_folder='landingschema'
target_db="s_aadp_foundation"

#initialize s3 client 
s3_client =boto3.client('s3')

def hash_key(cast_sdf,map_keyval):
    for key in map_keyval:
            cast_sdf=cast_sdf.withColumn(key, md5(concat_ws("|", *map_keyval[key])))
    hashed_key_sdf=cast_sdf
    return hashed_key_sdf


def add_auditcol(hashed_key_sdf,*audit_cols,job_name):
    ## input arguments passed : data frame , audit columns and job name
    ## Code to return the data frame with audit columns
    auditcol_sdf=hashed_key_sdf.withColumn("foundation_program_nam",lit(job_name))\
              .withColumn("foundation_ins_dt",current_timestamp())
    return auditcol_sdf

def add_ym_partition(base_sdf,partition_col):
    ## input arguments passed : data frame and the column on which partition will be performed
    ## Code to return data frame with partitioned column
    # ym_partitioned_sdf=base_sdf.withColumn('transaction_month', concat(year(to_timestamp(col(partition_col)))\
                                                                       # ,format_string("%02d",month(to_timestamp(col(partition_col)))))) 
    ym_partitioned_sdf=base_sdf.withColumn('transaction_month', concat(year(col(partition_col))\
                                                                       ,format_string("%02d",month(col(partition_col)))))
    return ym_partitioned_sdf
    

def create_base_frame(applymapping_gdf,map_keyval,glueContext,spark,job_name,source_tbl,audit_cols=['foundation_program_nam','foundation_ins_dt']):
    ## This is the master function from which we are calling hashed key function,audit col function
    ##converting the dynamic dataframe to dataframe
    applymapping_sdf=applymapping_gdf.toDF()
    cast_sdf=build_querystring(applymapping_sdf,source_tbl,spark)
    hashed_key_sdf=hash_key(cast_sdf,map_keyval)
    auditcol_sdf=add_auditcol(hashed_key_sdf,*audit_cols,job_name=job_name)
    key_cols=list(map_keyval.keys())
    target_schema = [*key_cols, *applymapping_sdf.columns, *audit_cols] 
    ordered_sdf = auditcol_sdf.select(*target_schema)
    base_gdf = DynamicFrame.fromDF(ordered_sdf, glueContext, "ordered_sdf")
    base_gdf.printSchema()
    base_gdf.toDF().show(truncate=False)
    return base_gdf

    
def create_ym_partitoned_frame(base_gdf,partition_col,glueContext):
    ##This is the master function from which we are calling hashed key function,audit col function and partitioned function
    ##converting the dynamic dataframe to dataframe
    base_sdf=base_gdf.toDF()
    ym_partitioned_sdf=add_ym_partition(base_sdf,partition_col)
    ym_partitioned_gdf = DynamicFrame.fromDF(ym_partitioned_sdf, glueContext, "ym_partitioned_sdf")
    ym_partitioned_gdf.printSchema()
    ym_partitioned_gdf.toDF().show(truncate=False)
    return ym_partitioned_gdf



def write_partitioned_sink(target_gdf,target_tbl,partitionKeys,glueContext,source_prefix):
    sink_gdf = glueContext.write_dynamic_frame.from_options(frame = target_gdf, connection_type = "s3", connection_options = {"path": landing_parent_bucket+source_prefix+"//"+target_tbl, "partitionKeys": partitionKeys}, format = "parquet", transformation_ctx = "sink_gdf")


def write_nonpartitioned_sink(base_gdf,target_tbl,glueContext,source_prefix):
    sink_gdf = glueContext.write_dynamic_frame.from_options(frame = base_gdf, connection_type = "s3", connection_options = {"path": landing_parent_bucket+source_prefix+"//"+target_tbl}, format = "parquet", transformation_ctx = "sink_gdf")



def build_querystring(applymapping_sdf,source_tbl,spark):
    # the below thing can be in one function and we can pass dataframe and source_tbl
    #applymapping_df=applymapping1.toDF()
    applymapping_sdf.createOrReplaceTempView(source_tbl)
    key=schema_folder+'/'+source_tbl+'.json'
    s3_clientjsonobj = s3_client.get_object(Bucket=schema_bucket_name, Key=key)
    s3_clientjsonschema = s3_clientjsonobj['Body'].read().decode('utf-8')
    # print("printing s3_clientdata")
    # print(s3_clientjsonschema)
    # print(type(s3_clientjsonschema))
    s3clientschemalist=json.loads(s3_clientjsonschema)
    # print("json loaded data")
    # print(s3clientschemalist)
    # print(type(s3clientschemalist))
    schemalist=s3clientschemalist[0]['fields']
    #print(schemalist)
    querystring='SELECT '
    def find_cast(i):
        switcher={
        'timestamp':'to_timestamp',
        'date':'to_date'
        }
        return switcher.get(i,"cast")
    for i in schemalist:
        print(find_cast(i['type']))
        str=find_cast(i['type'])
        if str=='cast':
            querystring=querystring+str+'('+i['name']+' as '+i['type']+') '+'as '+i['name']+','
        else:
            querystring=querystring+str+'('+i['name']+')'+' as '+i['name']+','
    
    querystring = querystring[:-1]+' from '+source_tbl
    print('--------------query string---------------')
    print(querystring)
    cast_df = spark.sql(querystring)
    cast_df.printSchema()
    print('--------------data type cast data frame---------------')
    cast_df.show(truncate=False)
    return cast_df