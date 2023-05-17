from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark=SparkSession.\
    builder.\
    appName('Customer').\
    getOrCreate()

schema=StructType([\
    StructField("CustomerID",IntegerType(),True), #True:nullable\
    StructField("ProductID",IntegerType(),True), \
    StructField("Amount",FloatType(),True)])

#Load the data/customer-orders.csv file as a DF with a schema
df=spark.read.schema(schema).csv("/Users/leesh970930/Desktop/sparkcourse/customer-orders.csv")
# df.printSchema()

#group by cust_id
#sum by amount_spent(bonus:round to 2 decimal places)
#sort by total spent
#show the result
GroupedCustomerID=df.groupBy("CustomerID"). \
                        agg(func.round(func.sum("Amount"),2).alias("total")) \
                        .sort("total")

GroupedCustomerID.show()

