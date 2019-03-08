import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

artists = sc.textFile("/Users/fridastrandkristoffersen/Downloads/datasets/artists.csv")
artistmap = artists.map(lambda line: line.split(","))
birthdate = artistmap.map(lambda x: x[4]).map(int)
rdd1 = birthdate.sortBy(lambda x: x, True)

print(rdd1.take(1))
