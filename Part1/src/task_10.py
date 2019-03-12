import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
import csv

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

sqlc=SQLContext(sc)

#Loading the album dataset and converting it to a dataframe
album_df = sqlc.read.csv("/datasets/albums.csv")
album_df2 = album_df.toDF("id","artist_id","album_title","genre","year_of_pub",\
"num_of_tracks","num_of_sales","rolling_stone_critic","mtv_critic","music_maniac_critic")

#Registering the dataframe as a table
sqlc.registerDataFrameAsTable(album_df2, "albumTable")

#Loading the artist dataset and converting it to a dataframe
artist_df = sqlc.read.csv("/datasets/artists.csv")
artist_df2 = artist_df.toDF("id","real_name","art_name","role","year_of_birth","country","city","email","zip_code")

#Registering the dataframe as a table
sqlc.registerDataFrameAsTable(artist_df2, "artistTable")

#Oppgave a
#sqlc.sql("SELECT COUNT(DISTINCT id) FROM artistTable").show()
#Output: 50000

#Oppgave b
#sqlc.sql("SELECT COUNT(DISTINCT id) FROM albumTable").show()
#Output: 100000

#Oppgave c
#sqlc.sql("SELECT COUNT(DISTINCT genre) FROM albumTable").show()
#Output: 38

#Oppgave d
#sqlc.sql("SELECT COUNT(DISTINCT country) FROM artistTable").show()
#Output: 249

#Oppgave e
#sqlc.sql("SELECT MIN(year_of_pub) AS Min_year_of_pub FROM albumTable").show()
#Output: 2000

#Oppgave f
#sqlc.sql("SELECT MAX(year_of_pub) AS Max_year_of_pub FROM albumTable").show()
#Output: 2019

#Oppgave g
#sqlc.sql("SELECT MIN(year_of_birth) AS Min_year_of_birth FROM artistTable").show()
#Output: 1955

#Oppgave h
#sqlc.sql("SELECT MAX(year_of_birth) AS Max_year_of_birth FROM artistTable").show()
#Output: 2000
