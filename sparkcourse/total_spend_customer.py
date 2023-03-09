from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("customer_spent")
sc=SparkContext(conf=conf)

def extractCustomerInfo(input):
    line=input.split(',')
    customerID=line[0]
    amount=line[2]
    #형변환 주의!!
    return (int(customerID),float(amount))

input=sc.textFile("/Users/leesh970930/Desktop/sparkcourse/customer-orders.csv")
#split each comma-delimited line into fields
mappedInput=input.map(extractCustomerInfo)
totalByCustomer=mappedInput.reduceByKey(lambda x, y: x + y)
#정렬
#1.먼저 map의 key value를 뒤집는다.
flipped=totalByCustomer.map(lambda x:(x[1],x[0]))
#2.sortByKey()로 정렬
totalByCustomerSorted=flipped.sortByKey()
#Use reduceByKey to add up amount spent by customer ID
res=totalByCustomerSorted.collect();

for r in res:
    print(r)
    

