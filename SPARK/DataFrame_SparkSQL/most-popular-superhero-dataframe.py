from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
'''
marvel 코믹북 유니버스에서 가장 많이 등장한 슈퍼히어로 찾기
'''
spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("/Users/leesh970930/Desktop/sparkcourse/Marvel-names.txt")
'''
+---+--------------------+
| id|                name|
+---+--------------------+
|  1|24-HOUR MAN/EMMANUEL|
|  2|3-D MAN/CHARLES CHAN|
|  3|    4-D MAN/MERCURIO|
|  4|             8-BALL/|
|  5|                   A|
+---+--------------------+
'''
lines = spark.read.text("/Users/leesh970930/Desktop/sparkcourse/Marvel-graph.txt")
'''
+--------------------+
|               value|
+--------------------+
|5988 748 1722 375...|
|5989 4080 4264 44...|
|5982 217 595 1194...|
|5983 1165 3836 43...|
|5980 2731 3712 15...|
+--------------------+
'''

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
'''
+----+-----------+
|  id|connections|
+----+-----------+
| 691|          6|
|1159|         11|
|3959|        142|
|1572|         35|
|2294|         14|
+----+-----------+
'''
    
mostPopular = connections.sort(func.col("connections").desc()).first() #Row(id='859', connections=1933)
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first() #Row(name='CAPTAIN AMERICA')

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

