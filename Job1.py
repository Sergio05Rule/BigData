# coding=utf-8

from pyspark import SparkConf, SparkContext
import sys
import time

start = time.time()

if len(sys.argv) != 3:
    print("\n \n Usage: <input path> <output path>\n \n")
    sys.exit(-1)

conf = SparkConf().setMaster("local").setAppName("Job1")
sc = SparkContext(conf=conf)


RDDUserArtists = sc.textFile(sys.argv[1], 1)

RDD= RDDUserArtists.flatMap(lambda line: line.split("\n")) \
    .map(lambda line: (line.split("\t")[1], line.split("\t")[5])) \
    .filter(lambda line: "artistID" not in line) \
    .map(lambda (y, z): ( y.encode("ascii", "ignore"), z.encode("ascii", "ignore"))) \
    .map(lambda (y, z): ( int(y), int(z))) \
    .filter(lambda (y, z): z > 2006) \
    .map(lambda (x,y): (x,1))\
    .reduceByKey(lambda x,y: x+y) \
    .map(lambda (x,y): (y,x)) \
    .sortByKey(ascending=False) \
    .map(lambda (x,y): (y,x)) #\
    #.take(10)

end = time.time()

#RDD = RDD.take(10)

end1 = time.time()

#RDD = sc.parallelize(RDD)
#RDD.saveAsTextFile(sys.argv[2])

end2 = time.time()

print("\n\n\n\n---EXECUTION TIME---\n" + str(end - start) + "\n\n\n\n")
print("\n\n\n\n---EXECUTION TIME---\n" + str(end1 - start) + "\n\n\n\n")
print("\n\n\n\n---EXECUTION TIME---\n" + str(end2 - start) + "\n\n\n\n")

sc.stop()