# 1. DataFrame 생성
- SparkSession 객체를 사용해 DataFrame을 생성할 수 있다.
- SparkSession 객체는 pyspark shell을 실행할 때 spark 라는 이름으로 미리 생성된다.

## Row 객체를 사용해 생성하기
- row : DataFrame에서의 한 행

```python
df = spark.createDataFrame([
    Row(name='정현진', age=26, birth=date(1997,8,30)),
    Row(name='하명도', age=21, birth=date(2001,9,6)),
    Row(name='이상엽', age=22, birth=date(2000,10,8))
])

df.show()

'''
+------+---+----------+
|  name|age|     birth|
+------+---+----------+
|정현진| 26|1997-08-30|
|하명도| 21|2001-09-06|
|이상엽| 22|2000-10-08|
+------+---+----------+
'''
```

## Schema 확인
```python
df.printSchema()

'''
root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- birth: date (nullable = true)
'''
```

## schema를 명시하여 DataFrame 생성
- 튜플에 데이터를 저장하고 스키마를 직접 지정

```python
df2 = spark.createDataFrame([
    ('김경민', 17, date(2005, 10, 11)),
    ('김도은', 18, date(2004, 12, 25)),
    ('김민석', 11, date(2011, 1, 10))
], schema='name string, age int, birth date')

df2.show()
df2.printSchema()

'''
+------+---+----------+
|  name|age|     birth|
+------+---+----------+
|김경민| 17|2005-10-11|
|김도은| 18|2004-12-25|
|김민석| 11|2011-01-10|
+------+---+----------+

root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- birth: date (nullable = true)
'''
```

## StructType 객체를 사용해 Schema 지정
```python
schema = StructType([
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("birth", DateType(), False),
    StructField("is_pass", BooleanType(), False)
])

df3 = spark.createDataFrame([
    ('손지수', 22, date(2011, 6, 8), True),
    ('유승종', 21, date(2009, 8, 28), True),
    ('윤병우', 23, date(2022, 3, 3), True)
])

df3.show()
df3.printSchema()

'''
+------+---+----------+----+
|    _1| _2|        _3|  _4|
+------+---+----------+----+
|손지수| 22|2011-06-08|true|
|유승종| 21|2009-08-28|true|
|윤병우| 23|2022-03-03|true|
+------+---+----------+----+

root
 |-- _1: string (nullable = true)
 |-- _2: long (nullable = true)
 |-- _3: date (nullable = true)
 |-- _4: boolean (nullable = true)
'''
```

## 중첩스키마적용
```python
data = [
    ('이서정', 21, date(2000, 11, 11), ('010', '1111', '2222')),
    ('이선희', 25, date(1999, 11, 11), ('010', '2222', '3333')),
    ('정주연', 23, date(2244, 6, 23), ('010', '3333', '4444'))
]

schema = StructType([
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("birth", DateType(), False),
    StructField("phone", StructType([
        StructField("phone1", StringType(), False),
        StructField("phone2", StringType(), False),
        StructField("phone3", StringType(), False)
    ]), False, metadata = {'desc':'user phone number'})
])

df4 = spark.createDataFrame(data = data, schema = schema)
df4.show()
df4.printSchema()

schema_of_json = df4.schema.json
print(schema_of_json)

'''
+------+---+----------+-----------------+
|  name|age|     birth|            phone|
+------+---+----------+-----------------+
|이서정| 21|2000-11-11|{010, 1111, 2222}|
|이선희| 25|1999-11-11|{010, 2222, 3333}|
|정주연| 23|2244-06-23|{010, 3333, 4444}|
+------+---+----------+-----------------+

root
 |-- name: string (nullable = false)
 |-- age: integer (nullable = false)
 |-- birth: date (nullable = false)
 |-- phone: struct (nullable = false)
 |    |-- phone1: string (nullable = false)
 |    |-- phone2: string (nullable = false)
 |    |-- phone3: string (nullable = false)

<bound method DataType.json of StructType(List(StructField(name,StringType,false),StructField(age,IntegerType,false),StructField(birth,DateType,false),StructField(phone,StructType(List(StructField(phone1,StringType,false),StructField(phone2,StringType,false),StructField(phone3,StringType,false))),false)))>
'''
```

## Pandas DataFrame으로 생성
```python
pandas_df = pd.DataFrame({
    'name':['정현진', '한병현', '홍효정'],
    'age':[20, 21, 22],
    'birth':[date(2000, 1, 1), date(2001, 2, 2), date(2005, 5, 5)]
})

pandas_df
df5 = spark.createDataFrame(pandas_df)
df5.show()

'''
	name	age	birth
0	정현진	20	2000-01-01
1	한병현	21	2001-02-02
2	홍효정	22	2005-05-05
+------+---+----------+
|  name|age|     birth|
+------+---+----------+
|정현진| 20|2000-01-01|
|한병현| 21|2001-02-02|
|홍효정| 22|2005-05-05|
+------+---+----------+
'''
```

## DataFrame -> Pandas
```python
pandas_df2 = df5.toPandas()
pandas_df2

'''
name	age	birth
0	정현진	20	2000-01-01
1	한병현	21	2001-02-02
2	홍효정	22	2005-05-05
'''
```

## DataFrame -> pyspark.pandas
```python
pandas_df3 = df5.to_pandas_on_spark()
pandas_df3

'''
name	age	birth
0	정현진	20	2000-01-01
1	한병현	21	2001-02-02
2	홍효정	22	2005-05-05
'''
```

## 외부파일을 사용해 DataFrame 생성
```python
class_df = spark.read.csv('/dataframe/a_class_info.csv', header = True)
class_df.show(3)

'''
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     6OL| ANKYI|           20|   Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|   Urban| Non-public|     Standard|
|     2B1| CCAAW|           18|Suburban| Non-public| Experimental|
+--------+------+-------------+--------+-----------+-------------+
'''
```

## DataFrame 컬럼
- withColumn

```python
'''
+------+---+----------+-----------------+
|  name|age|     birth|            phone|
+------+---+----------+-----------------+
|이서정| 21|2000-11-11|{010, 1111, 2222}|
|이선희| 25|1999-11-11|{010, 2222, 3333}|
|정주연| 23|2244-06-23|{010, 3333, 4444}|
+------+---+----------+-----------------+
'''

df4 = df4.withColumn('성별', lit(''))
df4.show(3)

# 기본값 지정, withColumn 사용 시 기존의 컬럼과 컬럼명이 같으면 덮어쓴다
df4 = df4.withColumn('성별', lit('F'))
df4.show(3)

# 조건에 따라 다른 컬럼 값을 가지도록 컬럼을 추가
# when - otherwise
temp = df4.withColumn('성별', when(df4.age < 23, '여성')
                                         .when(df4.age == 23, 'F')
                                         .otherwise('Female'))

temp.show(3)

'''
+------+---+----------+-----------------+----+
|  name|age|     birth|            phone|성별|
+------+---+----------+-----------------+----+
|이서정| 21|2000-11-11|{010, 1111, 2222}|    |
|이선희| 25|1999-11-11|{010, 2222, 3333}|    |
|정주연| 23|2244-06-23|{010, 3333, 4444}|    |
+------+---+----------+-----------------+----+

+------+---+----------+-----------------+----+
|  name|age|     birth|            phone|성별|
+------+---+----------+-----------------+----+
|이서정| 21|2000-11-11|{010, 1111, 2222}|   F|
|이선희| 25|1999-11-11|{010, 2222, 3333}|   F|
|정주연| 23|2244-06-23|{010, 3333, 4444}|   F|
+------+---+----------+-----------------+----+

+------+---+----------+-----------------+------+
|  name|age|     birth|            phone|  성별|
+------+---+----------+-----------------+------+
|이서정| 21|2000-11-11|{010, 1111, 2222}|  여성|
|이선희| 25|1999-11-11|{010, 2222, 3333}|Female|
|정주연| 23|2244-06-23|{010, 3333, 4444}|     F|
+------+---+----------+-----------------+------+
'''
```

## column 내용 변경
```python
df4 = df4.withColumn('성별', lit(''))
df4.show(3)

# 기본값 지정, withColumn 사용 시 기존의 컬럼과 컬럼명이 같으면 덮어쓴다
df4 = df4.withColumn('성별', lit('F'))
df4.show(3)

'''
+------+---+----------+-----------------+----+
|  name|age|     birth|            phone|성별|
+------+---+----------+-----------------+----+
|이서정| 21|2000-11-11|{010, 1111, 2222}|    |
|이선희| 25|1999-11-11|{010, 2222, 3333}|    |
|정주연| 23|2244-06-23|{010, 3333, 4444}|    |
+------+---+----------+-----------------+----+

+------+---+----------+-----------------+----+
|  name|age|     birth|            phone|성별|
+------+---+----------+-----------------+----+
|이서정| 21|2000-11-11|{010, 1111, 2222}|   F|
|이선희| 25|1999-11-11|{010, 2222, 3333}|   F|
|정주연| 23|2244-06-23|{010, 3333, 4444}|   F|
+------+---+----------+-----------------+----+
'''
```

## column 이름 변경
```python
temp = temp.withColumnRenamed('성별', 'gender')
temp.show()

'''
+------+---+----------+-----------------+------+
|  name|age|     birth|            phone|gender|
+------+---+----------+-----------------+------+
|이서정| 21|2000-11-11|{010, 1111, 2222}|  여성|
|이선희| 25|1999-11-11|{010, 2222, 3333}|Female|
|정주연| 23|2244-06-23|{010, 3333, 4444}|     F|
+------+---+----------+-----------------+------+
'''
```

## column 삭제
```python
temp = temp.drop('gender')
temp.show()

'''
+------+---+----------+-----------------+
|  name|age|     birth|            phone|
+------+---+----------+-----------------+
|이서정| 21|2000-11-11|{010, 1111, 2222}|
|이선희| 25|1999-11-11|{010, 2222, 3333}|
|정주연| 23|2244-06-23|{010, 3333, 4444}|
+------+---+----------+-----------------+
'''
```