# Set Operation

## 집합연산
- union : 합집합, 결과집합에서 중복되는 행 제거하지 않음
- unionAll : 2.0버전이후 union으로 대체됨, union과 동일한 함수
- exceptAll : 차집합
- intersect : 교집합, 결과집합에서 중복되는 행 제거
- intersectAll : 교집합, 결과집합에서 중복되는 행 제거하지 않음

## union, unionAll
- union과 unionAll은 SQL의 unionAll과 같은 개념

```python
# 학생 수가 30명 초과인 반과, 학생 수가 16명 미만인 합집합을 구해보자
cdf.where(cdf.class_std_cnt >= 30) \
.union(cdf.where(cdf.class_std_cnt < 16)) \
.show()

'''
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     18K| GOOBU|           31|   Urban|     Public|     Standard|
|     A93| VVTVA|           30|   Urban|     Public| Experimental|
|     YTB| VVTVA|           30|   Urban|     Public| Experimental|
|     Q0E| ZOWMK|           30|   Urban|     Public| Experimental|
|     QA2| ZOWMK|           30|   Urban|     Public|     Standard|
|     ZBH| ZOWMK|           30|   Urban|     Public|     Standard|
|     IQN| CCAAW|           15|Suburban| Non-public| Experimental|
|     197| FBUMG|           14|   Rural| Non-public| Experimental|
|     JGD| FBUMG|           14|   Rural| Non-public| Experimental|
|     MDE|  null|           10|   Rural| Non-public| Experimental|
|     SSP| UUUQX|           15|Suburban| Non-public|     Standard|
|     KR1| VHDHF|           15|   Rural| Non-public| Experimental|
+--------+------+-------------+--------+-----------+-------------+
'''
```

## intersect, intersectAll
- 교집합
- intersect는 중복을 제거하고, intersectAll은 중복을 제거하지 않는다.

```python
# intersect와 intersectAll의 차이를 확인하기 위해 중복데이터를 추가하여 새로운 DF로 생성
temp = cdf.collect()

temp.append({
    'class_cd':'A33'
    ,'school':'CIMBB'
    ,'class_std_cnt':'19'
    ,'loc':'Urban'
    ,'school_type':'Non-public'
    ,'teaching_type':'Standard'    
})

temp_df = spark.createDataFrame(temp)

# 학교이름이 C로 시작하는 클래스와 학교 위치가 도시인 클래스간의 교집합을 구하시오
# intersect
temp_df.where(temp_df.school.like("C%")) \
.intersect(temp_df.where(temp_df.loc == 'Urban')) \
.orderBy(temp_df.class_cd).show()

# intersectAll
temp_df.where(temp_df.school.like("C%")) \
.intersectAll(temp_df.where(temp_df.loc == 'Urban')) \
.orderBy(temp_df.class_cd).show()

'''
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     1Q1| CUQAM|           28|Urban|     Public|     Standard|
|     A33| CIMBB|           19|Urban| Non-public|     Standard|
|     BFY| CUQAM|           27|Urban|     Public|     Standard|
|     EID| CIMBB|           21|Urban| Non-public|     Standard|
|     HUJ| CIMBB|           17|Urban| Non-public| Experimental|
|     OMI| CUQAM|           28|Urban|     Public|     Standard|
|     PC6| CIMBB|           17|Urban| Non-public|     Standard|
|     X6Z| CUQAM|           24|Urban|     Public| Experimental|
+--------+------+-------------+-----+-----------+-------------+

+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     1Q1| CUQAM|           28|Urban|     Public|     Standard|
|     A33| CIMBB|           19|Urban| Non-public|     Standard|
|     A33| CIMBB|           19|Urban| Non-public|     Standard|
|     BFY| CUQAM|           27|Urban|     Public|     Standard|
|     EID| CIMBB|           21|Urban| Non-public|     Standard|
|     HUJ| CIMBB|           17|Urban| Non-public| Experimental|
|     OMI| CUQAM|           28|Urban|     Public|     Standard|
|     PC6| CIMBB|           17|Urban| Non-public|     Standard|
|     X6Z| CUQAM|           24|Urban|     Public| Experimental|
+--------+------+-------------+-----+-----------+-------------+
'''
```

## exceptAll
- 차집합

```python
temp_df.where(temp_df.school.like("C%")) \
.exceptAll(temp_df.where(temp_df.loc == 'Urban')) \
.orderBy(temp_df.class_cd).show()

'''
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     2B1| CCAAW|           18|Suburban| Non-public| Experimental|
|     EPS| CCAAW|           20|Suburban| Non-public| Experimental|
|     IQN| CCAAW|           15|Suburban| Non-public| Experimental|
|     PGK| CCAAW|           21|Suburban| Non-public|     Standard|
|     UHU| CCAAW|           16|Suburban| Non-public| Experimental|
|     UWK| CCAAW|           19|Suburban| Non-public|     Standard|
+--------+------+-------------+--------+-----------+-------------+
'''
```