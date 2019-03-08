import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

albums = sc.textFile("/Users/fridastrandkristoffersen/Downloads/datasets/albums.csv")

albumslist = albums.map(lambda line: line.split(","))
albumslist2 = albumslist.map(lambda x: x[1]).map(int)
country_count = albumslist2.map(lambda x: (x, 1))
rdd1 = country_count.reduceByKey(lambda n, m: n+m)
rdd2 = rdd1.sortBy(lambda x: (x[1], x[0]), False)
rdd2.coalesce(1).saveAsTextFile("result_4.tsv")
