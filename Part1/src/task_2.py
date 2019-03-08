import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

#Loading the artists into a RDD and also splitting on "," for each element in the original file.
#artists = sc.textFile("/Users/fridastrandkristoffersen/Downloads/datasets/artists.csv").map(lambda line: line.split(","))
artists = sc.textFile("/Users/haraldaarskog/Google\ Drive/Workspace/git/BigDataGit/datasets/artists.csv").map(lambda line: line.split(","))

#Selecting the birth dates from the data set
birthDates = artists.map(lambda x: int(x[4]))

#Sorting the birth dates by ascending order
birthDates_sorted = birthDates.sortBy(lambda x: x, True)

#Taking out the element on top in which has the lowest birth date
print(birthDates_sorted.take(1))

#Output: 1955
