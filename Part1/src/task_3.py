import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

#Loading the artists into a RDD and also splitting on "," for each element in the original file.
artists = sc.textFile("/Users/haraldaarskog/Google\ Drive/Workspace/git/BigDataGit/datasets/artists.csv").map(lambda line: line.split(","))
#Adding a key-value pair for each instance in the rdd. Each instance get a value of 1.
country_count = artists.map(lambda x: (x[5], 1))

#Using the reduceByKey tranformation in order to count the number of artists from each country
total_country_count = country_count.reduceByKey(lambda n, m: n+m)

#Sorting on the number of artists from each country in a descending and adding a "\t" in order to make it tab separated
sorted_country_count = total_country_count.sortBy(lambda x: x[1], False).map(lambda x: x[0]+"\t"+str(x[1]))

#Saving the sorted_country_count into a text file
sorted_country_count.coalesce(1).saveAsTextFile("/Users/haraldaarskog/Google Drive/Workspace/git/BigDataGit/Part1/Output/result_3")
