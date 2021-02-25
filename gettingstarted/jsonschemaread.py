from pyspark.sql import SparkSession   #Import SparkSession from pyspark.sql
from pyspark.sql.functions import month, year, concat_ws, md5, current_timestamp, to_timestamp # import month and year
from pyspark.sql import functions as F
import os
import itertools
os.environ['HADOOP_HOME'] = "C:\\winutils"
my_spark = SparkSession.builder.getOrCreate()

df = my_spark.read.format("csv").option("header", "true").load("C:\\Users\pjena\Downloads\olist_orders_dataset_v1.csv")
df.printSchema()
df.show(truncate=False)
df.createOrReplaceTempView("list_orders")

import json
with open('C://pyprogram/order.json') as f:
  data = json.load(f)

print("json loaded data")
print(data)
print(type(data))

schemalist=data[0]['fields']
#source_table_name=data[0]['source_table_name']
source_table_name='list_orders'
print(schemalist)
print(source_table_name)

querystring='SELECT '
def find_cast(i):
    switcher={
    'timestamp':'to_timestamp',
    'date':'to_date',
    'yy/MM/dd':'yy/MM/dd'
    }
    return switcher.get(i,"cast")
for i in schemalist:
    print(find_cast(i['type']))
    str=find_cast(i['type'])
    if str=='cast':
        querystring=querystring+str+'('+i['name']+' as '+i['type']+') '+'as '+i['name']+','
    elif str=='yy/MM/dd':
        querystring = querystring + 'to_date(cast(unix_timestamp(trim(' + i['name'] + '), "yy/MM/dd") as timestamp)) as  ' + i['name'] + ','
    else:
        querystring=querystring+str+'('+i['name']+')'+'as '+i['name']+','

querystring = querystring[:-1]+' from '+source_table_name
print('-----------------------------')
print(querystring)

# sqlDF = my_spark.sql('''SELECT cast(order_id as string)
# ,cast(customer_id as string)
# ,cast(order_status as string)
# ,to_timestamp(order_purchase_timestamp)
# ,to_timestamp(order_approved_at)
# ,to_date(order_delivered_carrier_date)
# ,to_date(order_delivered_customer_date)
# ,to_date(order_estimated_delivery_date) from list_orders''')

sqlDF = my_spark.sql(querystring)
sqlDF.printSchema()
sqlDF.show(truncate=False)