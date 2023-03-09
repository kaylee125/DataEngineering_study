
#정규식 활용
import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    # \W+ :단어로 분리, 모든 문장부호 처리
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/Users/leesh970930/Desktop/sparkcourse/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
