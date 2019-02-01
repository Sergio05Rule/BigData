# coding=utf-8
from pyspark import SparkConf, SparkContext
import sys

'''
if len(sys.argv) =! 2:
    print ("Errore nei path assegnati")
    exit (-1)
'''

conf = SparkConf().setMaster("local").setAppName("Job3")
sc = SparkContext(conf=conf)
RDDtextFile = sc.textFile("/Users/memoriessrls/Desktop/Documents/Università/Ingegneria Infromatica Magistrale/Big Data/Terza prova (Spark)/WordCount.dat")
#RDDtextFile = sc.textFile(sys.argv[1])

counts = RDDtextFile.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

output = counts.collect()

for word in output:
    print (word)

sc.stop()
'''
# Creates a DataFrame having a single column named "line"
df = textFile.map(lambda r: Row(r)).toDF(["line"])
errors = df.filter(col("line").like("%ERROR%"))
# Counts all the errors
errors.count()
# Counts errors mentioning MySQL
errors.filter(col("line").like("%MySQL%")).count()
# Fetches the MySQL errors as an array of strings
errors.filter(col("line").like("%MySQL%")).collect()
'''