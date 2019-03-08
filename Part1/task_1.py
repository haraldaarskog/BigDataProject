import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

#initializing the album
#albums1 = sc.textFile("/Users/fridastrandkristoffersen/Downloads/datasets/albums.csv").map(lambda line: line.split(","))
albums2 = sc.textFile("/Users/haraldaarskog/Google\ Drive/Workspace/git/BigDataGit/datasets/albums.csv ").map(lambda line: line.split(","))
genre = albums2.map(lambda x: x[3])

print(genre.distinct().collect())

print(genre.distinct().count())
