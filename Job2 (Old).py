# coding=utf-8
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.functions import desc
import pyspark.sql.functions as F
import sys

if len(sys.argv) != 3:
    print("\n \n Usage: <input path> <output path>\n \n")
    sys.exit(-1)

conf = SparkConf().setMaster("local").setAppName("Job2")
sc = SparkContext(conf=conf)

RDDUserArtists = sc.textFile(sys.argv[1])

counts = RDDUserArtists.flatMap(lambda line: line.split("\n")) \
    .map(lambda line: (line.split("\t")[0], line.split("\t")[1], line.split("\t")[2])) \
    .filter(lambda line: "userID" not in line) \
    .map(lambda (x, y, z): (x.encode("ascii", "ignore"), y.encode("ascii", "ignore"), z.encode("ascii", "ignore"))) \
    .map(lambda (x, y, z): (int(x), int(y), int(z))) \
    .map(lambda x: Row(userID=x[0], artistID=x[1], weight=x[2]))

# Create a DataFrame by applying createDataFrame on RDD with the help of sqlContext.
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(counts)

print(df.sort(("artistID"), desc("weight")).show(20))


def myConcat(*cols):
    concat_columns = []
    for c in cols[:-1]:
        concat_columns.append(F.coalesce(c, F.lit("*")))
        concat_columns.append(F.lit(" "))
    concat_columns.append(F.coalesce(cols[-1], F.lit("*")))
    return F.concat(*concat_columns)


df_text = df.sort(("artistID"), desc("weight")).withColumn("combined", myConcat(*df.columns)).select("combined")

df_text.coalesce(1).write.format("text").option("header", "False").mode("append").save(sys.argv[2])

sc.stop()