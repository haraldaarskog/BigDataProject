import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
import csv

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

sqlc=SQLContext(sc)

#ALBUMS
album_df = sqlc.read.csv("/Users/haraldaarskog/Google\ Drive/Big\ data-arkitektur/Prosjekt/datasets/albums.csv")
album_df2 = album_df.toDF("id","artist_id","album_title","genre","year_of_pub",\
"num_of_tracks","num_of_sales","rolling_stone_critic","mtv_critic","music_maniac_critic")
sqlc.registerDataFrameAsTable(album_df2, "albumTable")

#ARTISTS
artist_df = sqlc.read.csv("/Users/haraldaarskog/Google\ Drive/Big\ data-arkitektur/Prosjekt/datasets/artists.csv")
artist_df2 = artist_df.toDF("id","real_name","art_name","role","year_of_birth","country","city","email","zip_code")
sqlc.registerDataFrameAsTable(artist_df2, "artistTable")

#Oppgave a
#sqlc.sql("SELECT COUNT(DISTINCT id) FROM artistTable").show()

#Oppgave b
#sqlc.sql("SELECT COUNT(DISTINCT id) FROM albumTable").show()

#Oppgave c
#sqlc.sql("SELECT COUNT(DISTINCT genre) FROM albumTable").show()

#Oppgave d
#sqlc.sql("SELECT COUNT(DISTINCT country) FROM artistTable").show()

#Oppgave e
#sqlc.sql("SELECT MIN(year_of_pub) AS Min_year_of_pub FROM albumTable").show()

#Oppgave f
#sqlc.sql("SELECT MAX(year_of_pub) AS Max_year_of_pub FROM albumTable").show()

#Oppgave g
#sqlc.sql("SELECT MIN(year_of_birth) AS Min_year_of_birth FROM artistTable").show()

#Oppgave h
sqlc.sql("SELECT MAX(year_of_birth) AS Max_year_of_birth FROM artistTable").show()
