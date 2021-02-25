
# https://stackoverflow.com/questions/47665491/pyspark-throws-typeerror-textfile-missing-1-required-positional-argument-na

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.shell import spark

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
source_df = spark.read.csv(r"C:\Users\Psibbal\PycharmProjects\caa-data-load\input\Source.csv")
print (source_df.count())
target_df = spark.read.csv(r"C:\Users\Psibbal\PycharmProjects\caa-data-load\input\Target.csv")
print (target_df.count())
source_df.sort(['ITEM', 'LOC'])
target_df.sort(['ITEM', 'LOC'])

print(source_df.loc[1])
print(target_df.loc[1])
