import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext
from operator import add
import time
start = time.time()


def recommend(user, k, file, output):
    #Loading dataset
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    tweets = sc.textFile(file)

    #Splitting dataset on tab and mapping in key-value pair:(user(word))
    tweets2=tweets.map(lambda line: line.split("\t")).flatMapValues(lambda x: x.split(' ')).map(lambda x: (x[0],(x[1])))
    #flatMapping the values and splitting on space, remapping to key-value pair

    #finding alle the tweets for our input user
    userTweet1=tweets2.lookup(user)
    #saving the tweet as a dictionary and count
    userTweet2= sc.parallelize(userTweet1).countByValue()

    #filtering out the input user and tweets not in the tweets of the user. Then mapping in key-value pair: ((user, word), 1)
    corp777=tweets2.filter(lambda x: x[0]!=user and x[1] in userTweet1).map(lambda x: ((x[0],x[1]),1))

    #reducing
    corp4=corp777.reduceByKey(lambda n,m: n+m)

    #finding the minimum frequency
    corp5=corp4.map(lambda x: (x[0][0], min(userTweet2[x[0][1]],x[1])))


    corp6=corp5.reduceByKey(lambda n,m: n+m)
    compare2=corp6.sortBy(lambda x: x[0], True).sortBy(lambda x: x[1], False)

    x = sc.parallelize(compare2.take(k)).map(lambda x: x[0]+"\t"+str(x[1]))
    x.coalesce(1).saveAsTextFile(output)

recommend("rachele_m13", 5, "tweets.tsv", "output.tsv")
end = time.time()
print(end - start)
