# coding=utf-8
from pyspark import SparkConf, SparkContext
import time
import sys

if len(sys.argv) != 3:
    print("\n \n Usage: <input path> <output path>\n \n")
    sys.exit(-1)

start = time.time()

conf = SparkConf().setMaster("local").setAppName("UsersListening")
sc = SparkContext(conf=conf)

RDDUserListening = sc.textFile(sys.argv[1])
RDD = RDDUserListening.filter(lambda line: "userID" not in line)

counts = RDD.flatMap(lambda line: line.split("\n")) \
        .map(lambda line: (line.split("\t")[0], line.split("\t")[1], line.split("\t")[2])) \
        .map(lambda (x,y,z): (x.encode("ascii","ignore"),y.encode("ascii","ignore"),z.encode("ascii","ignore"))) \
        .map(lambda (x,y,z): (int(x),int(y),int(z))) \
        .map(lambda (x,y,z): (y, (x, z))) \
        .groupByKey()


def takeSecond(elem):
    return elem[1]

deflist=[]

for x in counts.collect():
        list = []
        for y in x[1]:
                list.append((y[0],y[1]))
        list.sort(key=takeSecond,reverse=True)
        for z in list:
                deflist.append((x[0],z[0],z[1]))

RDD1 = sc.parallelize(deflist)

end = time.time()
print("\n\n\n\n" + str(end - start) + "\n\n\n\n")

RDD1.saveAsTextFile(sys.argv[2])

sc.stop()