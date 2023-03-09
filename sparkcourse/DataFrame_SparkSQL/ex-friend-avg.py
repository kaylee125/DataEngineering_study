from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark=SparkSession.builder.appName('appname').getOrCreate()

people=spark.read.option("header","true").option("inferSchema","true")\
.csv("/Users/leesh970930/Desktop/sparkcourse/fakefriends-header.csv")

# people.printSchema()

#나이별 친구 평균 몇명인지 구하기
#필요한 컬럼만 select: 나이,친구수
friendsByage=people.select("age","friends")

#나이별로 그룹화한 다음 친구의 평균 구하기
friendsByage.groupBy("age").avg("friends").show()

#나이를 기준으로 정렬
friendsByage.groupBy("age").avg("friends").sort("age").show()

# #소숫점 정리 및 alias 설정
#func를 사용하는 이유:파이썬 내장함수와 커맨드가 겹칠 수 있어서 pyspark의 functions을 func로 별칭을 설정하고 func.커맨드로 사용
friendsByage.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()

#spark 세션 stop
spark.stop()