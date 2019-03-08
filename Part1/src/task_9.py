import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

albums = sc.textFile("/Users/haraldaarskog/Google\ Drive/Big\ data-arkitektur/Prosjekt/datasets/albums.csv")
albumslist = albums.map(lambda line: line.split(","))
albumslist2 = albumslist.map(lambda x: (int(x[1]),(float(x[8]), 1)))

artists = sc.textFile("/Users/haraldaarskog/Google\ Drive/Big\ data-arkitektur/Prosjekt/datasets/artists.csv")
artistmap= artists.map(lambda line: line.split(",")).filter(lambda x: x[5]=="Norway")
artistmap2 = artistmap.map(lambda x: (int(x[0]), (x[2])))

joined = albumslist2.join(artistmap2)

rating=joined.map(lambda x: (x[0], x[1][0][0]))
count=joined.map(lambda x: (x[0], x[1][0][1]))

rating2=rating.reduceByKey(lambda x,y: x+y)
count2=count.reduceByKey(lambda x,y: x+y)

countRatingJoin=rating2.join(count2)

artistJoin=countRatingJoin.join(artistmap2).map(lambda x: (x[1][1],"Norway",x[1][0][0]/x[1][0][1])).sortBy(lambda x: x[0], True).sortBy(lambda x: x[2], False)

print(artistJoin.collect())
#artistJoin.coalesce(1).saveAsTextFile("result_99.tsv")
