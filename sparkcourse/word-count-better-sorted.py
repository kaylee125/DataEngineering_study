import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/Users/leesh970930/Desktop/sparkcourse/book.txt")
words = input.flatMap(normalizeWords)
#map:모든 단어를 튜플형태로 변환 / reduceBykey: 키값 별로 value 합산
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
#단어들이 나타난 횟수로 정렬
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
