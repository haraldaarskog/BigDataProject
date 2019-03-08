import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
#Loading the albums dataset
albums = sc.textFile("/Users/haraldaarskog/Google\ Drive/Big\ data-arkitektur/Prosjekt/datasets/albums.csv").map(lambda line: line.split(","))
#making a new RDD with album id, artist id and average critic
album_critic = albums.map(lambda x: (int(x[1]),((x[0]), (float(x[7])+float(x[8])+float(x[9]))/3)))
#sorting the RDD based on average critics
album_critic_sorted = album_critic.sortBy(lambda x: x[1][1], False)

album_top10=sc.parallelize(album_critic_sorted.take(10))

#loading the artists dataset
artists = sc.textFile("/Users/haraldaarskog/Google\ Drive/Big\ data-arkitektur/Prosjekt/datasets/artists.csv").map(lambda line: line.split(","))
#Making a RDD consisting of only artist ID and country
artist_country = artists.map(lambda x: (int(x[0]), x[5]))
#joing album_top10 and artist_country on artist ID. Also mapping the result and sorting to make it look more nice
artist_album_join = artist_country.join(album_top10).map(lambda x: (x[1][1][0],x[1][1][1],x[1][0])).sortBy(lambda x: (x[1],x[0]), (False,True))
#adding the tabs berween the elements
artist_album_join_tsv=artist_album_join.map(lambda x: x[0]+"\t"+str(x[1])+"\t"+x[2])
#saving to text file
artist_album_join_tsv.coalesce(1).saveAsTextFile("/Users/haraldaarskog/Google Drive/Workspace/git/BigDataGit/Part1/Output/result_7")
