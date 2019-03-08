import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

artists = sc.textFile("/Users/fridastrandkristoffersen/Downloads/datasets/artists.csv")

artistlist = artists.map(lambda line: line.split(","))
country_count = artistlist.map(lambda x: (x[5], 1))
rdd1 = country_count.reduceByKey(lambda n, m: n+m)
rdd2 = rdd1.sortBy(lambda x: x[1], True)

rdd2.coalesce(1).saveAsTextFile("/Users/fridastrandkristoffersen/Downloads/datasets/result_3333.tsv")
