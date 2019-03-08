import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

"""
For the 10 albums of task 6, find the countries of their artists. Write a code
(named "task_7”) that writes the results in a TSV file in the form of
<album_id>tab<average_critic>tab<artist_country> and name it “result_7”.
"""


albums = sc.textFile("/Users/haraldaarskog/Google\ Drive/Big\ data-arkitektur/Prosjekt/datasets/albums.csv")

albumslist = albums.map(lambda line: line.split(","))
albumslist2 = albumslist.map(lambda x: (int(x[1]),((x[0]), (float(x[7])+float(x[8])+float(x[9]))/3)))
rdd2 = albumslist2.sortBy(lambda x: (x[1][1], x[1][0]), False)

rdd3=sc.parallelize(rdd2.take(10))

artists = sc.textFile("/Users/haraldaarskog/Google\ Drive/Big\ data-arkitektur/Prosjekt/datasets/artists.csv")

artistmap= artists.map(lambda line: line.split(","))
artistmap2 = artistmap.map(lambda x: (int(x[0]), x[5]))
joined = artistmap2.join(rdd3).sortBy(lambda x: (x[1][1],x[1][0]), False).map(lambda x: (x[1][1][0],x[1][1][1],x[1][0]))

joined.coalesce(1).saveAsTextFile("result_7.tsv")





"""
#finner gj.snkritikk av album og mapper på følgende måte:(artistid, (albumid, avg_critic))




joined2=joined.sortBy(lambda x: x[1][1][1], False).map(lambda line: (line[1][1][0], line[1][1][1], line[1][0]))

#print oppgave 7
print(sumCriticsPara.collect())
"""
"""
#oppgave 8
artistmap3=artistmap.map(lambda x: (int(x[0]), x[1]))
mtvCritics = albummap.map(lambda x: (int(x[1]), float(x[8])))
join8=artistmap3.join(mtvCritics).sortBy(lambda x: (x[1][1],x[1][0]), (False, True)).map(lambda x: (x[1][0],x[1][1]))
#print(join8.take(10))

#opppgave 9
artistsFromNorway = artistmap.filter(lambda x: x[5]=="Norway").map(lambda x: (x[0],x[2],x[5]))
albummap2=albummap.map(lambda x: (x[1],(x[7],x[8],x[9])))
join9=albummap2.join(artistsFromNorway).map(lambda x: (x[0],1))
jada=join9.reduce(lambda x: (x,1))
#print(join9.take(10))
"""
