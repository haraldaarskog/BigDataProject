import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

#Loading dataset
albums = sc.textFile("/datasets/albums.csv").map(lambda line: line.split(","))

#Calculating the average critic for each album ID
album_critic = albums.map(lambda x: ((x[0]), (float(x[7])+float(x[8])+float(x[9]))/3))
#sorting on critics
album_critic_sorted = album_critic.sortBy(lambda x: x[1], False)
#Taking out the top 10 albums with highest average critic and transforming it into a RDD
album_top10=sc.parallelize(album_critic_sorted.take(10)).map(lambda x: x[0]+"\t"+str(x[1]))

#saving to text file
album_top10.coalesce(1).saveAsTextFile("/Output/result_6")
