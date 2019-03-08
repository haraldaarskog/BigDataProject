import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

albums = sc.textFile("/Users/fridastrandkristoffersen/Downloads/datasets/albums.csv")

albumslist = albums.map(lambda line: line.split(","))
albumslist2 = albumslist.map(lambda x: ((x[0]), (float(x[7])+float(x[8])+float(x[9]))/3))
rdd2 = albumslist2.sortBy(lambda x: (x[1], x[0]), False)

rdd3=sc.parallelize(rdd2.take(10))
#gjør om lista til en rdd

rdd3.coalesce(1).saveAsTextFile("result_6.tsv")

""""
rdd1 = albumslist2.reduceByKey(lambda n, m: n+m)
rdd2 = rdd1.sortBy(lambda x: (x[1], x[0]), False)

"""
