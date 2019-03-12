import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
#loading the dataset into a RDD and also splitting every line on ",", making a two dimensional array
albums = sc.textFile("/datasets/albums.csv").map(lambda line: line.split(","))
#making a RDD of each genre and the associated number of sales
genre_sales = albums.map(lambda x: ((x[3]), int(x[6])))
#finding total sales for each distinct genre
genre_sales_total = genre_sales.reduceByKey(lambda n, m: n+m)
#sorting in a decending order based on number of sales and alphabetical if equal
genre_sales_sorted = genre_sales_total.sortBy(lambda x: (x[1], x[0]), False).map(lambda x: x[0]+"\t"+str(x[1]))
#saving to file
genre_sales_sorted.coalesce(1).saveAsTextFile("/Output/result_5")
