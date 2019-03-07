import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

albums = sc.textFile("/Users/fridastrandkristoffersen/Downloads/datasets/albums.csv")

albumsmap = albums.map(lambda line: line.split(","))
genre = albumsmap.map(lambda x: x[3])
print(genre.distinct().collect())
print(genre.distinct().count())
