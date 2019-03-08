import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

albums = sc.textFile("/Users/haraldaarskog/Google\ Drive/Big\ data-arkitektur/Prosjekt/datasets/albums.csv")
albumslist = albums.map(lambda line: line.split(",")).filter(lambda x: x[8]=="5")

albumslist2 = albumslist.map(lambda x: (int(x[1]),(x[8])))

artists = sc.textFile("/Users/haraldaarskog/Google\ Drive/Big\ data-arkitektur/Prosjekt/datasets/artists.csv")
artistmap= artists.map(lambda line: line.split(","))
artistmap2 = artistmap.map(lambda x: (int(x[0]), (x[2])))

#joined = albumslist2.join(artistmap2).map(lambda x: x[1][1]).distinct()

joined.coalesce(1).saveAsTextFile("result_88.tsv")

"""

#oppgave 8
artistmap3=artistmap.map(lambda x: (int(x[0]), x[1]))
mtvCritics = albummap.map(lambda x: (int(x[1]), float(x[8])))
join8=artistmap3.join(mtvCritics).sortBy(lambda x: (x[1][1],x[1][0]), (False, True)).map(lambda x: (x[1][0],x[1][1]))
#print(join8.take(10))
"""
