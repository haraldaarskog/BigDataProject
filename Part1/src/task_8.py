import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

#Loading the albums dataset
albums = sc.textFile("/datasets/albums.csv").map(lambda line: line.split(","))

#finding the albums with a MTV critics of 5
album_filtered=albums.filter(lambda x: x[8]=="5")

#Transforming album_filtered into a RDD with only artist ID and the MTV critic
album_mtvCritic = album_filtered.map(lambda x: (int(x[1]),(x[8])))

#loading the artists dataset
artists = sc.textFile("/datasets/artists.csv").map(lambda line: line.split(","))

#Transforming the artists RDD into a RDD with only artist ID and artist name. If the artistName is "" we replace it with the real name
artistName=artists.map(lambda x: (int(x[0]), (x[2])) if x[2]!="" else (int(x[0]), (x[1])))

#joining album_mtvCritic and artistName on artist ID. Then we only take the distinct artists and sorting in alphabetical order
album_artist_join = album_mtvCritic.join(artistName).map(lambda x: x[1][1]).distinct().sortBy(lambda x: x, True)

#saving to text file
album_artist_join.coalesce(1).saveAsTextFile("/Output/result_8")
