import pyspark as py
from pyspark import SparkConf
from pyspark.context import SparkContext

def recommend(user, k, file, output):

    #Setting up the sparkContext
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

    #loading tweets from file
    tweetFile = sc.textFile(file)

    #Splitting the dataset on tab and mapping in key-value pairs in the following way: ((user,word),1)
    tweets=tweetFile.map(lambda line: line.split("\t")).flatMapValues(lambda x: x.split(' ')).map(lambda x: ((x[0],x[1]),1))

    #filtering out the input user and making a dictionairy with the count for every unique word in the tweets of the user
    userTweetDictionairy=tweets.filter(lambda x: x[0][0]==user).reduceByKey(lambda n,m:n+m).map(lambda x: (x[0][1],x[1])).collectAsMap()

    #filtering out the input user and every word not in the the tweets of the user
    filteredTweets=tweets.filter(lambda x: x[0][0]!=user and x[0][1] in userTweetDictionairy)

    #counting every (user, word)-key
    tweetCount=filteredTweets.reduceByKey(lambda n,m: n+m)

    #comparing each word frquency against the word frequency for the inputuser and choosing the minimum value
    #Then mapping to (user, count)
    tweetsCompared=tweetCount.map(lambda x: (x[0][0], min(userTweetDictionairy[x[0][1]],x[1])))

    #summing up all the word frequecies for every user
    tweetCount2=tweetsCompared.reduceByKey(lambda n,m: n+m)

    #sorting on frequency in descending order, and username in ascending order
    tweetsSorted=tweetCount2.sortBy(lambda x: x[0], True).sortBy(lambda x: x[1], False)

    #indexing every element in the rdd and filtering out the keys given in the input
    indexedTweets=tweetsSorted.zipWithIndex().filter(lambda key : key[1] < k and key[1] >=0)

    #inserting tabs and saving as text file
    indexedTweets.map(lambda x: x[0][0]+"\t"+str(x[0][1])).coalesce(1).saveAsTextFile(output)

recommend("rachele_m13", 5, "tweets.tsv", "output000.tsv")
#recommend("mary",3,"example.tsv","output1234.tsv")
