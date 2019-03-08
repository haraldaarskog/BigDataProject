import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

#loading the dataset into a RDD and also splitting every line on ",", making a two dimensional array
albums = sc.textFile("/Users/haraldaarskog/Google\ Drive/Workspace/git/BigDataGit/datasets/albums.csv").map(lambda line: line.split(","))

#Selecting only the genres into a new rdd
genres = albums.map(lambda x: x[3])

#Printing out the number of distinct genres using the distinct().count() action.
print(genres.distinct().count())

#Output: 38
