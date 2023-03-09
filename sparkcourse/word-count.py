from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/Users/leesh970930/Desktop/sparkcourse/book.txt")
#공백 기준 단어별로 나눔
words = input.flatMap(lambda x: x.split())
#유일한 value의 갯수를 카운트
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
