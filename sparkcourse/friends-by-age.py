from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("/Users/leesh970930/Desktop/sparkcourse/fakefriends.csv")
rdd = lines.map(parseLine) #(나이 ,친구수)
#rdd 데이터 집계
#1.mapvalue호출: dd.mapValues(lambda x: (x, 1))-> value값을(x,1)의 튜플 형태로 변경
#2.reduceByKey:나이별로 aggrigate
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#averagesByAge: 평균 팔로워수 계산: (33,(387,2))->(33,193.5)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
