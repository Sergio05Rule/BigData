# This example uses text8 file from http://mattmahoney.net/dc/text8.zip
# The file was downloaded, unzipped and split into multiple lines using
#
# wget http://mattmahoney.net/dc/text8.zip
# unzip text8.zip
# grep -o -E '\w+(\W+\w+){0,15}' text8 > text8_lines
# This was done so that the example can be run in local mode

'''
USAGE = ("bin/spark-submit --driver-memory 4g "
         "examples/src/main/python/mllib/word2vec.py text8_lines")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit("Argument for file not provided")
'''

#BigData - Final Project - v 0.20

from __future__ import print_function
from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec
from nltk.corpus import stopwords
import os

#Cambio variabili d'ambiente per lanciare correttamente PySpark
#/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home (Path Corretto)
print (os.environ.get('JAVA_HOME'))
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home"
print (os.environ.get('JAVA_HOME'))

#Path File in Input
#input_file_path = "/Users/memoriessrls/Downloads/text8_lines" \
input_file_path = "/Users/memoriessrls/Downloads/the odyssey.txt"
output_file_path = '/Users/memoriessrls/Downloads/the odyssey_filtered.txt'

#Rimuovo STOP WORDS
#stop_words = set(stopwords.words('english'))
stop_words=['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"]
file1 = open(input_file_path)
line = file1.read() # Use this to read file content as a stream:
words = line.split()
for r in words:
    if not r in stop_words:
        appendFile = open(output_file_path,'a')
        appendFile.write(" "+r)
        appendFile.close()

#Stopworld on w/ this istruction!
input_file_path = output_file_path

sc = SparkContext(appName='Word2Vec')
sc.setLogLevel('ERROR') #Diminuisco log dei WARN
inp = sc.textFile(input_file_path).map(lambda row: row.split(" "))

word2vec = Word2Vec()

#model = word2vec.fit(inp)
model = word2vec.setVectorSize(500).setWindowSize(10).fit(inp) #test finestra visione parola a 10

#Parola da trovare nel vocabolario
synonyms = model.findSynonyms('fear', 20)

for word, cosine_distance in synonyms:
    print("{}: {}".format(word, cosine_distance))

sc.stop()