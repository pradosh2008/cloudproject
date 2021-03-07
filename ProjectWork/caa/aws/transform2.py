# import additional libraries  
import pyspark
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import month,year ,concat_ws, md5 , current_timestamp ,lit,col ,to_timestamp
from awsglue.dynamicframe import DynamicFrame

def hash_primary_key(sdf,*key_list,source_tbl):
    ## Code to return the hashed value & key values
    ## Code to return only the hash value
    hashed_key_sdf=sdf.withColumn(source_tbl+'_key',md5(concat_ws("|",*key_list)))
    return hashed_key_sdf

def add_auditcol(hashed_key_sdf,*audit_cols,job_name):
    ## input arguments passed : data frame , audit columns and job name
    ## Code to return the data frame with audit columns
    auditcol_sdf=hashed_key_sdf.withColumn("foundation_program_nam",lit(job_name))\
              .withColumn("foundation_ins_dt",current_timestamp())
    return auditcol_sdf

def add_year_partition(auditcol_sdf,partition_col,source_tbl):
    ## input arguments passed : data frame and the column on which partition will be performed
    ## Code to return data frame with partitioned column
    year_partitioned_sdf=auditcol_sdf.withColumn(source_tbl+'_year', year(to_timestamp(col(partition_col))))
    return year_partitioned_sdf
	
def create_frame_with_partitions(applymapping_gdf,*key_list,audit_cols=['foundation_program_nam','foundation_ins_dt'],partition_col,job_name,source_tbl,glueContext):
    ##This is the master function from which we are calling hashed key function,audit col function and partitioned function
    ##converting the dynamic dataframe to dataframe
    sdf=applymapping_gdf.toDF()
    hashed_key_sdf=hash_primary_key(sdf,*key_list,source_tbl=source_tbl)
    auditcol_sdf=add_auditcol(hashed_key_sdf,*audit_cols,job_name=job_name)
    year_partitioned_sdf=add_year_partition(auditcol_sdf,partition_col,source_tbl)
    target_schema = [source_tbl + '_key', *sdf.columns, source_tbl + '_year', *audit_cols]
    ordered_sdf=year_partitioned_sdf.select(*target_schema)
    ordered_gdf = DynamicFrame.fromDF(ordered_sdf, glueContext, "ordered_sdf")
    ordered_gdf.printSchema()
    ordered_gdf.toDF().show(truncate=False)
    return ordered_gdf


def create_frame_no_partitions(applymapping_gdf,*key_list,audit_cols=['foundation_program_nam','foundation_ins_dt'],job_name,source_tbl,glueContext):
    ## This is the master function from which we are calling hashed key function,audit col function
    ##converting the dynamic dataframe to dataframe
    sdf=applymapping_gdf.toDF()
    hashed_key_sdf=hash_primary_key(sdf,*key_list,source_tbl=source_tbl)
    auditcol_sdf=add_auditcol(hashed_key_sdf,*audit_cols,job_name=job_name)
    target_schema = [source_tbl + '_key', *sdf.columns, *audit_cols]
    ordered_sdf = auditcol_sdf.select(*target_schema)
    ordered_gdf = DynamicFrame.fromDF(ordered_sdf, glueContext, "ordered_sdf")
    ordered_gdf.printSchema()
    ordered_gdf.toDF().show(truncate=False)
    return ordered_gdf
	

def write_partitioned_sink(ordered_gdf,target_tbl,source_tbl,glueContext):
    sink_gdf = glueContext.write_dynamic_frame.from_options(frame = ordered_gdf, connection_type = "s3", connection_options = {"path": "s3://asc-aadpdatalakelayer"+"//"+target_tbl, "partitionKeys": [source_tbl + '_year']}, format = "parquet", transformation_ctx = "sink_gdf")


def write_nonpartitioned_sink(ordered_gdf,target_tbl,glueContext):
    sink_gdf = glueContext.write_dynamic_frame.from_options(frame = ordered_gdf, connection_type = "s3", connection_options = {"path": "s3://asc-aadpdatalakelayer"+"//"+target_tbl}, format = "parquet", transformation_ctx = "sink_gdf")
