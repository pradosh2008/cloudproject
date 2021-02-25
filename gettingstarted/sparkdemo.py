#from pyspark import SparkConf,SparkContext  #Instead of SparkContext we can use SparkSession. As spark2.0 onwards it's better to use this unified version

from pyspark.sql import SparkSession   #Import SparkSession from pyspark.sql
from pyspark.sql.functions import month, year, concat_ws, md5, current_timestamp, to_timestamp # import month and year
from pyspark.sql import functions as F
import os
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

df = my_spark.read.format("csv").option("header", "true").load("C:\\Users\pjena\Downloads\olist_orders_dataset.csv")
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

# col_list = ['order_id','customer_id']
# schema_list= df.columns
# job_name=F.lit("first job")
# partition_col=F.col("order_purchase_timestamp")
#
# audit_cols=['foundation_program_nam','foundation_ins_dt']
#
#
# target_schema=['order_key',*schema_list,'order_year',*audit_cols]
#
# print("target schema")
# print(target_schema)




#df = df.withColumn('concatenated_cols',concat_ws("|",*col_list)).withColumn('order_key_new',md5(concat_ws("|",*col_list)))
#df.show(truncate=False)

# df1= df.withColumn('order_key_concatenated_str',concat_ws("|",*col_list))\
#     .withColumn('order_key',md5(concat_ws("|",*col_list)))\
#     .withColumn("order_version_concatenated_str", concat_ws("|", *schema_list))\
#     .withColumn("order_version_hash", md5(concat_ws("|", *schema_list)))\
#     .withColumn("order_year", year(to_timestamp(F.col("order_purchase_timestamp"))))\
#     .withColumn("foundation_program_nam",F.lit(job_name))\
#     .withColumn("foundation_ins_dt",current_timestamp().alias("foundation_ins_dt"))

# df1= df.withColumn('order_key_concatenated_str',concat_ws("|",*col_list))\
#     .withColumn('order_key',md5(concat_ws("|",*col_list)))\
#     .withColumn("order_version_concatenated_str", concat_ws("|", *schema_list))\
#     .withColumn("order_version_hash", md5(concat_ws("|", *schema_list)))\
#     .withColumn("order_year", year(to_timestamp(partition_col)))\
#     .withColumn("foundation_program_nam",F.lit(job_name))\
#     .withColumn("foundation_ins_dt",current_timestamp())


#df1=df1.select(*target_schema)
# df1.printSchema()
# df1.show(truncate=False)

#registering as a table and using pyspark sql
df.createOrReplaceTempView("list_orders")
#sqlDF = my_spark.sql("SELECT * FROM list_orders limit 10")
#sqlDF = my_spark.sql("SELECT order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date from list_orders limit 10")



sqlDF = my_spark.sql("""SELECT 
                       md5(order_id) as order_key
                      ,current_date() as as_of_date
                      ,cast(order_id as int)
                      ,cast(customer_id as int)
                      ,cast(order_status as string)
                      ,to_timestamp(order_purchase_timestamp) as order_purchase_timestamp
                      ,order_approved_at
                      ,to_date(order_delivered_carrier_date) as order_delivered_carrier_date
                      ,to_date(order_delivered_customer_date) as order_delivered_customer_date
                      from list_orders limit 10""")

sqlDF.printSchema()



#sqlDF.printSchema()
#sqlDF.show()

# sqlDF_year=sqlDF.withColumn("order_year", year('order_purchase_timestamp'))
#
# sqlDF_hashcol=sqlDF_year.withColumn("order_version_hash", md5(concat_ws("|", *sqlDF_year.columns)))
#
# sqlDF_ordered=sqlDF_hashcol.select("order_key","order_version_hash","as_of_date","order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date",current_timestamp().alias("foundation_ins_dt"))
#
# sqlDF_ordered.show(truncate=False)
# sqlDF_ordered.printSchema()

#quering on dataframe
#df.select("order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date").show()