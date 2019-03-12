import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

#loading the albums dataset
albums = sc.textFile("/datasets/albums.csv").map(lambda line: line.split(","))

#selecting artist ID and MTV critic from each row. Adding a value of 1 in order to later aggregate the elements
albumslist = albums.map(lambda x: (int(x[1]),(float(x[8]), 1)))

#loading the artists dataset
artists = sc.textFile("/datasets/artists.csv")

#selecting all the Norwegian artists
artists_norway= artists.map(lambda line: line.split(",")).filter(lambda x: x[5]=="Norway")

#replacing artistsName like "" with the real name of the artists
artistName=artists_norway.map(lambda x: (int(x[0]), (x[2])) if x[2]!="" else (int(x[0]), (x[1])))

#joining the albumlist with artistName on artist ID
album_artist_join = albumslist.join(artistName)

#finding the average rating for each artist by first summing up the ratings and album counts
rating=album_artist_join.map(lambda x: (x[0], x[1][0][0]))
rating2=rating.reduceByKey(lambda x,y: x+y)
count=album_artist_join.map(lambda x: (x[0], x[1][0][1]))
count2=count.reduceByKey(lambda x,y: x+y)

#joining the ratings and counts together so we can calculate the average
countRatingJoin=rating2.join(count2)

#joining the countRatingJoin with artistName in order to get the artist name
#Adding the country manually because we filtered the country earlier
#Sorting average critic in a descending order and the names in an ascending order
artistJoin=countRatingJoin.join(artistName).map(lambda x: (x[1][1],"Norway",x[1][0][0]/x[1][0][1])).sortBy(lambda x: x[0], True).sortBy(lambda x: x[2], False)
artistJoin_tsv = artistJoin.map(lambda x: x[0]+"\t"+x[1]+"\t"+str(x[2]))

#Writing to text file
artistJoin_tsv.coalesce(1).saveAsTextFile("/Output/result_9")
