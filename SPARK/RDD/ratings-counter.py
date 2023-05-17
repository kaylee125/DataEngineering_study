from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("/Users/leesh970930/Desktop/sparkcourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2]) #등급만 추출해서 rating RDD객체에 저장
result = ratings.countByValue() #countByvalue: 각 등급의 갯수를 튜플로 반환

#collections.OrderedDict: 파이썬 내장함수(튜플->key-value형태로 변환)
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
