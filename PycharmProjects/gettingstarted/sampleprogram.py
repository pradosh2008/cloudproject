from pyspark.sql.functions import month, year, concat_ws, md5, current_timestamp, to_timestamp
from pyspark.sql import functions as F

def hash_func(df,*key_list,prefix):
    ## Code to return the hashed value & key values
    #df_hash=df.select(md5(concat_ws("|",*key_list)).alias('Hash_key'),*key_list)
    ## Code to return only the hash value
    df_hash=df.withColumn(prefix+'_key',md5(concat_ws("|",*key_list)))
    return df_hash

def add_auditcol_func(df,*audit_cols,job_name):
    ## input arguments passed : dataframe , audit columns and job name
    ## Code to return the dataframe with audit columns
    df_hash=df.withColumn("foundation_program_nam",F.lit(job_name))\
              .withColumn("foundation_ins_dt",current_timestamp())
    return df_hash

def add_partition_func(df,partition_col,prefix):
    ## input arguments passed : dataframe and the column on which partition will be performed
    ## Code to return dataframe with partitoned column
    df_hash=df.withColumn("order_year", year(to_timestamp(F.col(partition_col))))
    return df_hash


def create_frame_with_partitions(df,*key_list,audit_cols=['foundation_program_nam','foundation_ins_dt'],partition_col,job_name,prefix):
    hashed_df=hash_func(df,*key_list,prefix=prefix)
    hashed_df.printSchema()
    hashed_df.show()
    audit_df=add_auditcol_func(hashed_df,*audit_cols,job_name=job_name)
    audit_df.printSchema()
    audit_df.show()
    partitoned_df=add_partition_func(audit_df,partition_col,prefix)
    partitoned_df.printSchema()
    partitoned_df.show()
    target_schema = [prefix + '_key', *df.columns, prefix + '_year', *audit_cols]
    ordered_df=partitoned_df.select(*target_schema)
    return ordered_df


def foreign_key_hash(df,map_val):
    for key in map_val:
            df=df.withColumn(key, md5(concat_ws("|", *map_val[key])))
            print(map_val[key])
            print(key)
    return df

# def hash_fkey_list(df,*fkey_list,fkey_col_name):
#     ordered_gdf=ordered_gdf.withColumn(fkey_col_name,md5(concat_ws("|",*fkey_list)))
#     return ordered_gdf



# def write_target(ordered_gdf,target):
#     sink_gdf = glueContext.write_dynamic_frame.from_options(frame = ordered_gdf, connection_type = "s3", connection_options = {"path": "s3://aadp-datalakelayer"+"//"+target}, format = "parquet", transformation_ctx = "sink_gdf")



# def order_frame(df,target_df,prefix,audit_cols):
#     target_schema = [prefix+'_key', *df.columns, prefix+'_year', *audit_cols]
#     return target_df.select(*target_schema)

