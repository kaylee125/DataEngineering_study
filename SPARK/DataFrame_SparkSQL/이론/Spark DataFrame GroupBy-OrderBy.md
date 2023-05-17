# GroupBy, OrderBy

## groupBy()

- groupBy  : 집계함수를 가지고 있는 GroupData 객체를 반환한다.  
             GroupData객체의 집계함수들을 사용해 grouping 된 데이터들의 집계결과를 저장하고 있는 DataFrame을 반환 받을 수 있다.

- **정렬시에 스트링으로 숫자 정렬을 하면 20과 20000을 같은 수준으로 보는 것과 같은 오류가 있다.**
- **이로인해 StructType을 Integer로 지정하여 정렬을 수행하는 등 알장은 데이터타입을 지정해줘야 한다.**

```python
print('지역별 교육타입별 학생 숫자를 구해보자.')
cdf.groupby(cdf.loc, cdf.teaching_type).agg(sum(cdf.class_std_cnt)).show()

print('''지역내 교육타입별 학생 숫자와 평균을 구해보자. 
단  지역내 교육타입별 학생 숫자의 총 합이 300미만인 데이터는 제외한다.''')
cdf.groupby(cdf.loc, cdf.teaching_type) \
        .agg(sum(cdf.class_std_cnt), avg('class_std_cnt')) \
        .where(sum(cdf.class_std_cnt) >= 300) \
        .show()

print('컬럼명이 sum(class_std_cnt) 이라니 너무 이상하다. 집계함수를 수행하고 별칭을 붙여보자')
cdf.groupby(cdf.loc, cdf.teaching_type) \
        .agg(sum(cdf.class_std_cnt).alias('total'), avg('class_std_cnt').alias('avg')) \
        .where(sum(cdf.class_std_cnt) >= 300) \
        .show()

# 반이 가장 많이 위치한 지역의 학생 수 총합과, 가장 적게 위치한 지역의 학생 수 총 합 간의 차이를 구해보자

base = cdf \
        .where(col('loc').isNotNull()) \
        .groupby(cdf.loc) \
        .agg( count(col('class_cd')).alias('cnt')
             , sum(col('class_std_cnt')).alias('tot')) 

base.show()

min_max_row = base.select(max('cnt'), min('cnt')).collect()
min_max_row

base.where(base.cnt.isin(min_max_row[0][0], min_max_row[0][1])) \
      .select(max(col('tot')) - min(col('tot'))).show()

'''
지역별 교육타입별 학생 숫자를 구해보자.
+--------+-------------+------------------+
|     loc|teaching_type|sum(class_std_cnt)|
+--------+-------------+------------------+
|   Rural| Experimental|               211|
|    null|         null|              null|
|   Urban|     Standard|               631|
|Suburban|     Standard|               433|
|   Rural|     Standard|               327|
|Suburban| Experimental|               284|
|   Urban| Experimental|               275|
+--------+-------------+------------------+

지역내 교육타입별 학생 숫자와 평균을 구해보자. 
단  지역내 교육타입별 학생 숫자의 총 합이 300미만인 데이터는 제외한다.
+--------+-------------+------------------+------------------+
|     loc|teaching_type|sum(class_std_cnt)|avg(class_std_cnt)|
+--------+-------------+------------------+------------------+
|   Urban|     Standard|               631| 24.26923076923077|
|Suburban|     Standard|               433|             21.65|
|   Rural|     Standard|               327|           20.4375|
+--------+-------------+------------------+------------------+

컬럼명이 sum(class_std_cnt) 이라니 너무 이상하다. 집계함수를 수행하고 별칭을 붙여보자
+--------+-------------+-----+-----------------+
|     loc|teaching_type|total|              avg|
+--------+-------------+-----+-----------------+
|   Urban|     Standard|  631|24.26923076923077|
|Suburban|     Standard|  433|            21.65|
|   Rural|     Standard|  327|          20.4375|
+--------+-------------+-----+-----------------+

+--------+---+---+
|     loc|cnt|tot|
+--------+---+---+
|   Urban| 37|906|
|Suburban| 34|717|
|   Rural| 28|538|
+--------+---+---+

[Row(max(cnt)=37, min(cnt)=28)]
+---------------------+
|(max(tot) - min(tot))|
+---------------------+
|                  368|
+---------------------+
'''
```

### groupBy - SQL

```python
print("지역별 교육타입별 학생 숫자를 구해보자.")
spark.sql('''
    select loc, teaching_type, sum(class_std_cnt)
    from class
    group by loc, teaching_type
''').show()

print('''지역내 교육타입별 학생 숫자와 평균을 구해보자. 
단  지역내 교육타입별 학생 숫자의 총 합이 300미만인 데이터는 제외한다.''')
spark.sql('''
    select loc, teaching_type, sum(class_std_cnt), avg(class_std_cnt)
    from class
    group by loc, teaching_type
    having sum(class_std_cnt) >= 300
''').show()

print('컬럼명이 sum(class_std_cnt) 이라니 너무 이상하다. 집계함수를 수행하고 별칭을 붙여보자')
spark.sql('''
    select loc, teaching_type, sum(class_std_cnt) as `학생수 합`, avg(class_std_cnt)
    from class
    group by loc, teaching_type
    having sum(class_std_cnt) >= 300
''').show()

'''
지역별 교육타입별 학생 숫자를 구해보자.
+--------+-------------+------------------+
|     loc|teaching_type|sum(class_std_cnt)|
+--------+-------------+------------------+
|   Rural| Experimental|               211|
|    null|         null|              null|
|   Urban|     Standard|               631|
|Suburban|     Standard|               433|
|   Rural|     Standard|               327|
|Suburban| Experimental|               284|
|   Urban| Experimental|               275|
+--------+-------------+------------------+

지역내 교육타입별 학생 숫자와 평균을 구해보자. 
단  지역내 교육타입별 학생 숫자의 총 합이 300미만인 데이터는 제외한다.
+--------+-------------+------------------+------------------+
|     loc|teaching_type|sum(class_std_cnt)|avg(class_std_cnt)|
+--------+-------------+------------------+------------------+
|   Urban|     Standard|               631| 24.26923076923077|
|Suburban|     Standard|               433|             21.65|
|   Rural|     Standard|               327|           20.4375|
+--------+-------------+------------------+------------------+

컬럼명이 sum(class_std_cnt) 이라니 너무 이상하다. 집계함수를 수행하고 별칭을 붙여보자
+--------+-------------+---------+------------------+
|     loc|teaching_type|학생수 합|avg(class_std_cnt)|
+--------+-------------+---------+------------------+
|   Urban|     Standard|      631| 24.26923076923077|
|Suburban|     Standard|      433|             21.65|
|   Rural|     Standard|      327|           20.4375|
+--------+-------------+---------+------------------+
'''
```

## orderBy()

```python
print('반 학생 숫자를 기준으로 내림차순 정렬하라')
cdf.select('*') \
    .orderBy(cdf.class_std_cnt, ascending = False).show(3)

cdf.select('*') \
    .orderBy(cdf.class_std_cnt.desc()).show(10)

print('loc를 기준으로 오름차순 정렬하라, 이때 같은 지역끼리는 학교이름을 기준으로 내림차순 정렬하라')
cdf.select('*') \
    .orderBy(cdf.loc, cdf.school.desc()).show(10)

print('학교 종류를 기준으로 오름차순 정렬하라, 만약 school_type이 null인 행이 있다면 제일 위로 오게 하라')
cdf.select('*') \
    .orderBy(cdf.school_type.asc_nulls_first()).show(10)

'''
반 학생 숫자를 기준으로 내림차순 정렬하라
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     18K| GOOBU|           31|Urban|     Public|     Standard|
|     Q0E| ZOWMK|           30|Urban|     Public| Experimental|
|     YTB| VVTVA|           30|Urban|     Public| Experimental|
+--------+------+-------------+-----+-----------+-------------+

+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     18K| GOOBU|           31|   Urban|     Public|     Standard|
|     A93| VVTVA|           30|   Urban|     Public| Experimental|
|     ZBH| ZOWMK|           30|   Urban|     Public|     Standard|
|     YTB| VVTVA|           30|   Urban|     Public| Experimental|
|     Q0E| ZOWMK|           30|   Urban|     Public| Experimental|
|     QA2| ZOWMK|           30|   Urban|     Public|     Standard|
|     7BL| VVTVA|           29|   Urban|     Public|     Standard|
|     1Q1| CUQAM|           28|   Urban|     Public|     Standard|
|     OMI| CUQAM|           28|   Urban|     Public|     Standard|
|     ROP| DNQDD|           28|Suburban|     Public| Experimental|
+--------+------+-------------+--------+-----------+-------------+

loc를 기준으로 오름차순 정렬하라, 이때 같은 지역끼리는 학교이름을 기준으로 내림차순 정렬하라
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     5SD|  null|         null| null|       null|         null|
|     4SZ|  null|         null| null|       null|         null|
|     6PP|  null|         null| null|       null|         null|
|     341| VKWQH|           18|Rural|     Public|     Standard|
|     D33| VKWQH|           21|Rural|     Public|     Standard|
|     GYM| VKWQH|           20|Rural|     Public|     Standard|
|     IEM| VKWQH|           22|Rural|     Public| Experimental|
|     DFQ| VKWQH|           19|Rural|     Public| Experimental|
|     J6X| VHDHF|           16|Rural| Non-public|     Standard|
|     KR1| VHDHF|           15|Rural| Non-public| Experimental|
+--------+------+-------------+-----+-----------+-------------+

학교 종류를 기준으로 오름차순 정렬하라, 만약 school_type이 null인 행이 있다면 제일 위로 오게 하라
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     5SD|  null|         null|    null|       null|         null|
|     4SZ|  null|         null|    null|       null|         null|
|     6PP|  null|         null|    null|       null|         null|
|     6OL| ANKYI|           20|   Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|   Urban| Non-public|     Standard|
|     2B1| CCAAW|           18|Suburban| Non-public| Experimental|
|     IQN| CCAAW|           15|Suburban| Non-public| Experimental|
|     UHU| CCAAW|           16|Suburban| Non-public| Experimental|
|     EPS| CCAAW|           20|Suburban| Non-public| Experimental|
|     PGK| CCAAW|           21|Suburban| Non-public|     Standard|
+--------+------+-------------+--------+-----------+-------------+
'''
```

### orderBy -SQL

```python
print('반 학생 숫자를 기준으로 내림차순 정렬하라')
spark.sql('''
    select * from class order by class_std_cnt desc
''').show(10)

print('loc를 기준으로 오름차순 정렬하라, 이때 같은 지역끼리는 학교이름을 기준으로 내림차순 정렬하라')
spark.sql('''
    select * from class order by loc asc, school desc
''').show(10)

print('학교 종류를 기준으로 오름차순 정렬하라, 만약 school_type이 null인 행이 있다면 제일 위로 오게 하라')
spark.sql('''
    select * from class order by school_type asc nulls first
''').show(10)

spark.sql('''
    select * from class order by school_type asc nulls last
''').show(10)

'''
반 학생 숫자를 기준으로 내림차순 정렬하라
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     18K| GOOBU|           31|   Urban|     Public|     Standard|
|     A93| VVTVA|           30|   Urban|     Public| Experimental|
|     ZBH| ZOWMK|           30|   Urban|     Public|     Standard|
|     YTB| VVTVA|           30|   Urban|     Public| Experimental|
|     Q0E| ZOWMK|           30|   Urban|     Public| Experimental|
|     QA2| ZOWMK|           30|   Urban|     Public|     Standard|
|     7BL| VVTVA|           29|   Urban|     Public|     Standard|
|     1Q1| CUQAM|           28|   Urban|     Public|     Standard|
|     OMI| CUQAM|           28|   Urban|     Public|     Standard|
|     ROP| DNQDD|           28|Suburban|     Public| Experimental|
+--------+------+-------------+--------+-----------+-------------+

loc를 기준으로 오름차순 정렬하라, 이때 같은 지역끼리는 학교이름을 기준으로 내림차순 정렬하라
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     5SD|  null|         null| null|       null|         null|
|     4SZ|  null|         null| null|       null|         null|
|     6PP|  null|         null| null|       null|         null|
|     341| VKWQH|           18|Rural|     Public|     Standard|
|     D33| VKWQH|           21|Rural|     Public|     Standard|
|     GYM| VKWQH|           20|Rural|     Public|     Standard|
|     IEM| VKWQH|           22|Rural|     Public| Experimental|
|     DFQ| VKWQH|           19|Rural|     Public| Experimental|
|     J6X| VHDHF|           16|Rural| Non-public|     Standard|
|     KR1| VHDHF|           15|Rural| Non-public| Experimental|
+--------+------+-------------+-----+-----------+-------------+

학교 종류를 기준으로 오름차순 정렬하라, 만약 school_type이 null인 행이 있다면 제일 위로 오게 하라
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     5SD|  null|         null|    null|       null|         null|
|     4SZ|  null|         null|    null|       null|         null|
|     6PP|  null|         null|    null|       null|         null|
|     6OL| ANKYI|           20|   Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|   Urban| Non-public|     Standard|
|     2B1| CCAAW|           18|Suburban| Non-public| Experimental|
|     IQN| CCAAW|           15|Suburban| Non-public| Experimental|
|     UHU| CCAAW|           16|Suburban| Non-public| Experimental|
|     EPS| CCAAW|           20|Suburban| Non-public| Experimental|
|     PGK| CCAAW|           21|Suburban| Non-public|     Standard|
+--------+------+-------------+--------+-----------+-------------+

+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     6OL| ANKYI|           20|   Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|   Urban| Non-public|     Standard|
|     2B1| CCAAW|           18|Suburban| Non-public| Experimental|
|     EPS| CCAAW|           20|Suburban| Non-public| Experimental|
|     IQN| CCAAW|           15|Suburban| Non-public| Experimental|
|     PGK| CCAAW|           21|Suburban| Non-public|     Standard|
|     UHU| CCAAW|           16|Suburban| Non-public| Experimental|
|     UWK| CCAAW|           19|Suburban| Non-public|     Standard|
|     A33| CIMBB|           19|   Urban| Non-public|     Standard|
|     EID| CIMBB|           21|   Urban| Non-public|     Standard|
+--------+------+-------------+--------+-----------+-------------+
'''