#This python script will be stored in the lib folder . The s3 path will be passed to the glue job.
# import additional libraries  
import pyspark,boto3
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import month,year ,concat_ws, md5 , current_timestamp ,lit,col ,to_timestamp,concat,format_string,trim,sha2,split,input_file_name,element_at
from awsglue.dynamicframe import DynamicFrame
import json

#initialize target parent bucket name and landing schea
target_bucket_name="s3-p-ascena-aadp-foundation"
schema_bucket_name='s3-p-ascena-code-repo'
source_bucket_name='s3-p-ascena-aadp-landing'
archive_bucket_name='s3-p-ascena-aadp-arc'

schema_folder='plus/glue/schema/foundation'
target_db="plus_foundation"

status=False

#initialize s3 client 
s3_client =boto3.client('s3')
s3_resource = boto3.resource('s3')

def hash_key(cast_sdf,map_keyval):
    for key in map_keyval:
            cast_sdf=cast_sdf.withColumn(key, md5(concat_ws("|", *map_keyval[key])))
    hashed_key_sdf=cast_sdf
    return hashed_key_sdf


def add_auditcol(hashed_key_sdf,*audit_cols,job_name):
    ## input arguments passed : data frame , audit columns and job name
    ## Code to return the data frame with audit columns
    auditcol_sdf=hashed_key_sdf.withColumn("foundation_program_nam",lit(job_name))\
              .withColumn("foundation_insert_tms",current_timestamp())
    return auditcol_sdf

def add_ym_partition(base_sdf,partition_col):
    ## input arguments passed : data frame and the column on which partition will be performed
    ## Code to return data frame with partitioned column
    ym_partitioned_sdf=base_sdf.withColumn('transaction_month', concat(year(col(partition_col))\
                                                                       ,format_string("%02d",month(col(partition_col)))))
    return ym_partitioned_sdf
    

def create_base_frame(source_sdf,map_keyval,glueContext,spark,job_name,source_tbl,audit_cols=['foundation_program_nam','foundation_insert_tms']):
    ## This is the master function from which we are calling hashed key function,audit col function
    ##converting the dynamic dataframe to dataframe
    logger = glueContext.get_logger()
    logger.info('create_base_frame .toDF()')
    #applymapping_sdf=applymapping_gdf.toDF()
    logger.info('create_base_frame build_querystring')
    cast_sdf=build_querystring(source_sdf,source_tbl,spark)
    logger.info('create_base_frame hash_key')
    hashed_key_sdf=hash_key(cast_sdf,map_keyval)
    logger.info('create_base_frame add_auditcol')
    auditcol_sdf=add_auditcol(hashed_key_sdf,*audit_cols,job_name=job_name)
    logger.info('create_base_frame creating new dynamic frame')
    key_cols=list(map_keyval.keys())
    target_schema = [*key_cols, *source_sdf.columns, *audit_cols] 
    ordered_sdf = auditcol_sdf.select(*target_schema)
    #base_gdf = DynamicFrame.fromDF(ordered_sdf, glueContext, "ordered_sdf")
    print('--------------print the target schema---------------')
    ordered_sdf.printSchema()
    #base_gdf.toDF().show(truncate=False)
    return ordered_sdf

    
def create_ym_partitoned_frame(base_sdf,partition_col,glueContext):
    ##This is the master function from which we are calling hashed key function,audit col function and partitioned function
    ##converting the dynamic dataframe to dataframe
    #base_sdf=base_gdf.toDF()
    ym_partitioned_sdf=add_ym_partition(base_sdf,partition_col)
    #ym_partitioned_gdf = DynamicFrame.fromDF(ym_partitioned_sdf, glueContext, "ym_partitioned_sdf")
    ym_partitioned_sdf.printSchema()
    #ym_partitioned_gdf.toDF().show(truncate=False)
    return ym_partitioned_sdf


def build_querystring(source_sdf,source_tbl,spark):
    # the below thing can be in one function and we can pass dataframe and source_tbl
    #applymapping_df=applymapping1.toDF()
    source_sdf.createOrReplaceTempView(source_tbl)
    key=schema_folder+'/'+source_tbl+'.json'
    s3_clientjsonobj = s3_client.get_object(Bucket=schema_bucket_name, Key=key)
    s3_clientjsonschema = s3_clientjsonobj['Body'].read().decode('utf-8')
    s3clientschemalist=json.loads(s3_clientjsonschema)
    schemalist=s3clientschemalist[0]['fields']
    querystring='SELECT '
    def find_cast(i):
        switcher={
        'timestamp':'to_timestamp',
        'date':'to_date',
        'yy/MM/dd':'yy/MM/dd',
        'lpad14':'lpad14'
        }
        return switcher.get(i,"cast")
    for i in schemalist:
        #print(find_cast(i['type']))
        str=find_cast(i['type'])
        if str=='cast':
            querystring=querystring+str+'('+i['name']+' as '+i['type']+') '+'as '+i['name']+','
        elif str=='yy/MM/dd':
            querystring = querystring + 'to_date(cast(unix_timestamp(' + i['name'] + ', "yy/MM/dd") as timestamp)) as  ' + i['name'] + ','
        elif str=='lpad14':
            querystring=querystring+'lpad(trim('+i['name']+'),14,"0") '+'as '+i['name']+','
        else:
            querystring=querystring+str+'('+i['name']+')'+' as '+i['name']+','
    
    querystring = querystring+'row_hashed_val,source_file_nam from '+source_tbl
    print('--------------query string---------------')
    print(querystring)
    cast_df = spark.sql(querystring)
    #print('--------------print the schema---------------')
    #cast_df.printSchema()
    #print('--------------show sample data for the source dataframe---------------')
    #cast_df.show(truncate=False)
    return cast_df
    

def write_to_sink(target_sdf,target_tbl,partitionKeys,glueContext,target_prefix):
    target_gdf = DynamicFrame.fromDF(target_sdf, glueContext, "target_sdf")
    logger = glueContext.get_logger()
    target_path="s3://"+target_bucket_name+target_prefix+"//"+target_tbl
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

    
def clean_sourcedf(source_prefix,source_tbl,spark):
    source_path="s3://"+source_bucket_name+source_prefix+"//"+source_tbl
    source_df=spark.read.option("delimiter","|").option("header", "true").csv(source_path+'//*')
    tempList = [] #Edit01
    for col in source_df.columns:
        new_name = col.strip().lower()
        new_name = "".join(new_name.split())
        new_name = new_name.replace('.','') # EDIT
        tempList.append(new_name) #Edit02
    #print(tempList)
    source_clean_df = source_df.toDF(*tempList)
    #return source_clean_df
    for col_c in source_clean_df.columns:
        #source_df = source_df.withColumn(col_c, func.ltrim(func.rtrim(source_df[col_c])))
        source_clean_df = source_clean_df.withColumn(col_c, trim(source_clean_df[col_c]))
        #source_clean_df = source_clean_df.withColumn(col_c, trim(col(col_c)))
    source_clean_df=source_clean_df.withColumn("source_file_nam", element_at(split(input_file_name(),"/"), -1))
    return source_clean_df    

def calc_rowhash(source_clean_df):
    cols = [c for c in source_clean_df.columns if c.lower() not in {'extract_ts','source_file_nam','batch_id'}]
    #source_clean_df.withColumn("row_sha2_val", concat_ws("||", *cols)).show(truncate=False)
    #source_clean_df.withColumn("row_sha2", sha2(concat_ws("||", *cols), 256)).show(truncate=False)
    source_distinct_df=source_clean_df.dropDuplicates(cols)
    source_hash_df=source_distinct_df.withColumn("row_hashed_val", sha2(concat_ws("||", *cols), 256))
    return source_hash_df

def inc_leftjoin_fdn(fdn_hash_df,inc_hash_df):
    inc_leftjoined_fdn_df = inc_hash_df.alias("a")\
    .join(fdn_hash_df.alias("b"), col("b.row_hashed_val") == col("a.row_hashed_val"), how='left')\
    .filter(col("b.row_hashed_val").isNull())\
    .select([col("a."+xx) for xx in inc_hash_df.alias("a").columns])
    #.show()
    #print(inc_leftjoined_fdn_df.count())
    #inc_leftjoined_fdn_df.show(truncate=False)
    return inc_leftjoined_fdn_df

def read_fdn(target_prefix,target_tbl,spark):
    target_path="s3://"+target_bucket_name+target_prefix+"//"+target_tbl
    #source_df=spark.read.option("delimiter","|").option("header", "true").csv(target_path+'//*')
    fdn_hash_df=spark.read.option("mergeSchema", "true").parquet(target_path)
    fdn_hash_df=fdn_hash_df.select("row_hashed_val")
    #fdn_hash_df.printSchema()
    #print(fdn_hash_df.count())
    return fdn_hash_df


def move_to_archive(status,source_prefix,source_tbl):
    if status == True:
        src = s3_resource.Bucket(source_bucket_name)
        dst = s3_resource.Bucket(archive_bucket_name)
        source_key="/".join(source_prefix.split('//')[1:])+'/'+source_tbl+'/'
        #all_objects = s3_client.list_objects(Bucket = source_bucket_name,Prefix=source_key) 
        #print(all_objects['Prefix'])
        for obj in src.objects.filter(Prefix=source_key):
            copy_source = { 'Bucket': source_bucket_name,
                           'Key': obj.key}
            #print(obj.key)
            arc_key = source_key + obj.key[len(source_key):]
            #print(arc_key)
            
            new_obj = dst.Object(arc_key)
            new_obj.copy(copy_source)
            
            if len(list(filter(None,arc_key.split('/')))) == 4:
                #print(arc_key)
                del_object = src.Object(arc_key)
                print("Deleting {} from {} bucket".format(arc_key,source_bucket_name))
                #del_object = s3_resource.Object(source_bucket_name, arc_key)
                response = del_object.delete()
                print("archival is done")
                #print(response)
    else:
        print("job has to be rerun")

