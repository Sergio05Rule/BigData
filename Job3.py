# coding=utf-8
from pyspark import SparkConf, SparkContext
import sys
import time

if len(sys.argv) != 3:
    print("\n \n Usage: <input path> <output path>\n \n")
    sys.exit(-1)

start = time.time()

conf = SparkConf().setMaster("local").setAppName("Job3")
sc = SparkContext(conf=conf)

RDDUserFriends = sc.textFile(sys.argv[1])

counts = RDDUserFriends.flatMap(lambda line: line.split("\n")) \
            .map(lambda line: (line.split("\t")[0],1)) \
            .reduceByKey(lambda x,y: x + y) \
            .sortBy((lambda a: a[1]),0) \
            .map(lambda (x,y): (x.encode("ascii","ignore"),y))

end = time.time()
print("\n\n\n\n---EXECUTION TIME---\n" + str(end - start) + "\n\n\n\n")

counts.saveAsTextFile(sys.argv[2])

sc.stop()