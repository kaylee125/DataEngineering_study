from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

#spark context 사용하지 않고 바로 spark.read하면 그 시점에서 df가 생성됨
#inferSchema:데이터의 스키마를 찾아주는 옵션
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("/Users/leesh970930/Desktop/sparkcourse/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
#select(기존 열,새로운 열)
people.select(people.name, people.age + 10).show()

spark.stop()

