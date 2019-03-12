import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

#loading the dataset into a RDD and also splitting every line on ",", making a two dimensional array
albums = sc.textFile("/Users/haraldaarskog/Google\ Drive/Workspace/git/BigDataGit/datasets/albums.csv").map(lambda line: line.split(","))

#Selecting all the artist IDs from the albums rdd and casting them as an integer.
album_artistID = albums.map(lambda x: int(x[1]))

#adding a key-value pair for each artist ID
album_count = album_artistID.map(lambda x: (x, 1))

#counting the number of albums for each artist
album_count_total = album_count.reduceByKey(lambda n, m: n+m)

#sorting and adding "\t" in order to make it tab separated
album_count_sorted = album_count_total.sortBy(lambda x: (x[1], x[0]), False).map(lambda x: str(x[0])+"\t"+str(x[1]))
album_count_sorted.coalesce(1).saveAsTextFile("/Users/haraldaarskog/Google Drive/Workspace/git/BigDataGit/Part1/Output/result_4")
