#from pyspark import SparkConf,SparkContext  #Instead of SparkContext we can use SparkSession. As spark2.0 onwards it's better to use this unified version

from pyspark.sql import SparkSession   #Import SparkSession from pyspark.sql
from pyspark.sql.functions import month, year, concat_ws, md5, current_timestamp, to_timestamp # import month and year
from pyspark.sql import functions as F
import os
from sampleprogram import *
import itertools
os.environ['HADOOP_HOME'] = "C:\\winutils"

#my_spark = SparkSession.builder.appName("SparkSessionExample").master(None).
#sc=SparkContext(master=None,appName="Spark Demo")
#print(sc.textFile("C:\\spark-2.4.3-bin-hadoop2.6\README.md").take(10))

my_spark = SparkSession.builder.getOrCreate() #Make a new SparkSession called my_spark using SparkSession.builder.getOrCreate()


# spark = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()


print(my_spark)  #Print my_spark to the console to verify it's a SparkSession
# Print the tables in the catalog
print(my_spark.catalog.listTables())

#df = my_spark.read.json("C:\\spark-2.4.3-bin-hadoop2.6\examples\src\main\\resources\people.json")
#df = my_spark.read.json("C:\\spark-2.4.3-bin-hadoop2.6\examples\src\main\\resources\people.json")

bc="olist_orders_dataset.csv"
parent_path="C:\\Users\pjena\Downloads"

df = my_spark.read.format("csv").option("header", "true").load(parent_path+"\\"+bc)
#df.show()

df.printSchema()
#df.select("order_id","customer_id").show()

#registering as a table and using pyspark sql
# df.createOrReplaceTempView("people")   ## Register the DataFrame as a SQL temporary view
# query = "FROM people SELECT * LIMIT 10"  # Don't change this query
# people10 = my_spark.sql(query)   # Get the first 10 rows of flights
# people10.show()
# or
# sqlDF = my_spark.sql("SELECT * FROM people")
# sqlDF.show()
#job_name=F.lit("first job")
#partition_col=F.col("order_purchase_timestamp")
#audit_cols=['foundation_program_nam','foundation_ins_dt']

key_list = ['order_id','customer_id']
schema_list= df.columns
job_name="first job"
partition_col="order_purchase_timestamp"
prefix='order'
#partition_suffix


#target_schema=['order_key',*schema_list,'order_year',*audit_cols]
#target_schema=",".join('order_key'+ schema_list + 'order_year'+ audit_cols)
#merged = list(itertools.chain(*target_schema))
#print("target schema")
#print(target_schema)
#prefix='order'

#fkey_list=['customer_id',['order_id','customer_id'],'order_id']

map_keyval = {'olist_orders_key':['order_id'],
           'customer_key':['customer_id'],
           'customer_order_key':['order_id','customer_id'],
           'order_test_key':['order_id']}

fkey_hashed_df=foreign_key_hash(df,map_keyval)
fkey_hashed_df.printSchema()
fkey_hashed_df.select('olist_orders_key','customer_key','customer_order_key','order_test_key').show(truncate=False)



# ordered_df=create_frame_with_partitions(df,*key_list,audit_cols=['foundation_program_nam','foundation_ins_dt'],partition_col=partition_col,job_name=job_name,prefix=prefix)
# print("------------ordered dataframe------------")
# ordered_df.printSchema()
# ordered_df.show(truncate=False)

# def hash_func(df,*key_list,prefix):
#     ## Code to return the hashed value & key values
#     #df_hash=df.select(md5(concat_ws("|",*key_list)).alias('Hash_key'),*key_list)
#     ## Code to return only the hash value
#     df_hash=df.select(md5(concat_ws("|",*key_list)).alias(prefix+'_key'))
#     return df_hash


