## select()
```python
'''
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     6OL| ANKYI|           20|   Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|   Urban| Non-public|     Standard|
|     2B1| CCAAW|           18|Suburban| Non-public| Experimental|
+--------+------+-------------+--------+-----------+-------------+
'''

# str
df_spark = class_df.select('school', 'loc')
df_spark.show(3)

# *
df_spark = class_df.select('*')
df_spark.show(3)

# list
df_spark = class_df.select(['class_cd', 'school'])
df_spark.show(3)

'''
+------+--------+
|school|     loc|
+------+--------+
| ANKYI|   Urban|
| ANKYI|   Urban|
| CCAAW|Suburban|
+------+--------+

+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     6OL| ANKYI|           20|   Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|   Urban| Non-public|     Standard|
|     2B1| CCAAW|           18|Suburban| Non-public| Experimental|
+--------+------+-------------+--------+-----------+-------------+

+--------+------+
|class_cd|school|
+--------+------+
|     6OL| ANKYI|
|     ZNS| ANKYI|
|     2B1| CCAAW|
+--------+------+
'''
```

## Column
```python
# Column : 내부에 DataFrame 연산을 위한 Expression을 가지고 있는 객체
col('school')

# 데이터프레임에서 원하는 컬럼을 반환
class_df.school

class_df.colRegex("`^school.*`")

lit('school')

'''
Column<'school'>
Column<'school'>
Column<'unresolvedregex()'>
Column<'school'>
'''

df_spark = class_df.select(col('school'))
df_spark.show(3)

df_spark = class_df.select(class_df.school)
df_spark.show(3)

df_spark = class_df.select(class_df.colRegex("`^school.*`"))
df_spark.show(3)

df_spark = class_df.select(lit('school'))
df_spark.show(3)

'''
+------+
|school|
+------+
| ANKYI|
| ANKYI|
| CCAAW|
+------+

+------+
|school|
+------+
| ANKYI|
| ANKYI|
| CCAAW|
+------+

+------+-----------+
|school|school_type|
+------+-----------+
| ANKYI| Non-public|
| ANKYI| Non-public|
| CCAAW| Non-public|
+------+-----------+

+------+
|school|
+------+
|school|
|school|
|school|
+------+
'''
```

## Column 연산
- 산술연산 :  +  -  *  /  %
- 비교연산 : <  >  <=  >=  ==  !=
- 논리연산 : &  |

```python
# DataFrame의 select 메서드에 새로운 컬럼을 전달인자로 보낼 경우 해당 컬럼이 추가된 새로운 DataFrame이 생성된다.
class_df.select(class_df.school, class_df.class_cd, (class_df.class_std_cnt >= 10) & (class_df.class_std_cnt < 20)).show(3)
class_df.select(class_df.school, class_df.class_cd, ((class_df.class_std_cnt >= 10) & (class_df.class_std_cnt < 20)).alias('is_big')).show(3)
col_is_big = ((class_df.class_std_cnt >= 10) & (class_df.class_std_cnt < 20)).alias('is_big')
class_df.select(class_df.school
                , class_df.class_cd
                , col_is_big).show(3)

'''
+------+--------+------------------------------------------------+
|school|class_cd|((class_std_cnt >= 10) AND (class_std_cnt < 20))|
+------+--------+------------------------------------------------+
| ANKYI|     6OL|                                           false|
| ANKYI|     ZNS|                                           false|
| CCAAW|     2B1|                                            true|
+------+--------+------------------------------------------------+

+------+--------+------+
|school|class_cd|is_big|
+------+--------+------+
| ANKYI|     6OL| false|
| ANKYI|     ZNS| false|
| CCAAW|     2B1|  true|
+------+--------+------+

+------+--------+------+
|school|class_cd|is_big|
+------+--------+------+
| ANKYI|     6OL| false|
| ANKYI|     ZNS| false|
| CCAAW|     2B1|  true|
+------+--------+------+
'''
```

## Column 메서드
```python
school = class_df.school
class_cd = class_df.class_cd
std_cnt = class_df.class_std_cnt

std_cnt.asc()
class_df.select(school, class_cd, std_cnt).sort(std_cnt.asc()).show(100)
class_df.select(school, class_cd, std_cnt).sort(std_cnt.desc()).show(100)

'''
Column<'class_std_cnt ASC NULLS FIRST'>
+------+--------+-------------+
|school|class_cd|class_std_cnt|
+------+--------+-------------+
|  null|     4SZ|         null|
|  null|     6PP|         null|
|  null|     5SD|         null|
|  null|     MDE|           10|
| FBUMG|     197|           14|
| FBUMG|     JGD|           14|
| VHDHF|     KR1|           15|
| CCAAW|     IQN|           15|
| UUUQX|     SSP|           15|
| CCAAW|     UHU|           16|
+------+--------+-------------+

+------+--------+-------------+
|school|class_cd|class_std_cnt|
+------+--------+-------------+
| GOOBU|     18K|           31|
| VVTVA|     A93|           30|
| VVTVA|     YTB|           30|
| ZOWMK|     Q0E|           30|
| ZOWMK|     QA2|           30|
| ZOWMK|     ZBH|           30|
| VVTVA|     7BL|           29|
| CUQAM|     1Q1|           28|
| QOQTS|     0N7|           28|
| CUQAM|     OMI|           28|
+------+--------+-------------+
'''
```

## select 연습
```python
from pyspark.sql import functions as F
cdf = class_df

# 모든 row의 class_cd, school, loc, school_type, teaching_type을 출력하시오.
# school_type의 컬럼명은 '공립/사립여부' 로 표시합니다.
# hint : alias()
cdf.select('class_cd', 'school', 'loc', class_df.school_type.alias('공립/사립여부'), 'teaching_type').show()

# class_cd, school, scale 을 출력하시오.
# scale 컬럼은 반의 인원수가 15명 미만이면 small, 15명 이상 25명 미만이면 middle, 25명 이상이면 big 값을 가집니다.
# hint : when()
class_df.printSchema()
class_df.select('class_cd', 'school', (when(class_df.class_std_cnt < 15, 'small') 
                                            .when((class_df.class_std_cnt >= 15) & (class_df.class_std_cnt < 25), 'middle')
                                            .otherwise('big')).alias('scale')).show(3)

# class_cd, school, class_std_cnt를 출력하시오
# class_std_cnt 컬럼의 값들은 string타입으로 형변환합니다.
# hint : cast()
class_df.select('class_cd', 'school', class_df.class_std_cnt.cast('string')).printSchema()

'''
+--------+------+--------+-------------+-------------+
|class_cd|school|     loc|공립/사립여부|teaching_type|
+--------+------+--------+-------------+-------------+
|     6OL| ANKYI|   Urban|   Non-public|     Standard|
|     ZNS| ANKYI|   Urban|   Non-public|     Standard|
|     2B1| CCAAW|Suburban|   Non-public| Experimental|
|     EPS| CCAAW|Suburban|   Non-public| Experimental|
|     IQN| CCAAW|Suburban|   Non-public| Experimental|
|     PGK| CCAAW|Suburban|   Non-public|     Standard|
|     UHU| CCAAW|Suburban|   Non-public| Experimental|
|     UWK| CCAAW|Suburban|   Non-public|     Standard|
|     A33| CIMBB|   Urban|   Non-public|     Standard|
|     EID| CIMBB|   Urban|   Non-public|     Standard|
|     HUJ| CIMBB|   Urban|   Non-public| Experimental|
|     PC6| CIMBB|   Urban|   Non-public|     Standard|
|     1Q1| CUQAM|   Urban|       Public|     Standard|
|     BFY| CUQAM|   Urban|       Public|     Standard|
|     OMI| CUQAM|   Urban|       Public|     Standard|
|     X6Z| CUQAM|   Urban|       Public| Experimental|
|     2AP| DNQDD|Suburban|       Public|     Standard|
|     PW5| DNQDD|Suburban|       Public| Experimental|
|     ROP| DNQDD|Suburban|       Public| Experimental|
|     ST7| DNQDD|Suburban|       Public|     Standard|
+--------+------+--------+-------------+-------------+
only showing top 20 rows

root
 |-- class_cd: string (nullable = true)
 |-- school: string (nullable = true)
 |-- class_std_cnt: string (nullable = true)
 |-- loc: string (nullable = true)
 |-- school_type: string (nullable = true)
 |-- teaching_type: string (nullable = true)

+--------+------+------+
|class_cd|school| scale|
+--------+------+------+
|     6OL| ANKYI|middle|
|     ZNS| ANKYI|middle|
|     2B1| CCAAW|middle|
+--------+------+------+
only showing top 3 rows

root
 |-- class_cd: string (nullable = true)
 |-- school: string (nullable = true)
 |-- class_std_cnt: string (nullable = true)
'''
```

### Select - SQL
```python
# dataFrame을 테이블로 등록
class_df.createOrReplaceTempView('class')

# 모든 row의 class_cd, school, loc, school_type, teaching_type을 출력하시오.
# school_type의 컬럼명은 '공립/사립여부' 로 표시합니다.
# alias 지정시 ``(백틱) 사용
spark.sql('''
    select class_cd, school, loc, school_type as `공립/사립여부`, teaching_type from class
''').show(3)


# class_cd, school, scale 을 출력하시오.
# scale 컬럼은 반의 인원수가 15명 미만이면 small, 15명 이상 25명 미만이면 middle, 25명 이상이면 big 값을 가집니다.
spark.sql('''
    select class_cd, school,
        case when class_std_cnt < 15 then 'small'
               when class_std_cnt >= 15 and class_std_cnt < 25 then 'middle'
               else 'big'
        end as scale
    from class
''').show(3)


# class_cd, school, class_std_cnt를 출력하시오
# class_std_cnt 컬럼의 값들은 string타입으로 형변환합니다.
# spark built-in-fnc 의 cast()
spark.sql('''
    select class_cd, school, cast(class_std_cnt as string) from class
''').printSchema()

'''
+--------+------+--------+-------------+-------------+
|class_cd|school|     loc|공립/사립여부|teaching_type|
+--------+------+--------+-------------+-------------+
|     6OL| ANKYI|   Urban|   Non-public|     Standard|
|     ZNS| ANKYI|   Urban|   Non-public|     Standard|
|     2B1| CCAAW|Suburban|   Non-public| Experimental|
+--------+------+--------+-------------+-------------+
only showing top 3 rows

+--------+------+------+
|class_cd|school| scale|
+--------+------+------+
|     6OL| ANKYI|middle|
|     ZNS| ANKYI|middle|
|     2B1| CCAAW|middle|
+--------+------+------+
only showing top 3 rows

root
 |-- class_cd: string (nullable = true)
 |-- school: string (nullable = true)
 |-- class_std_cnt: string (nullable = true)
'''
```

## where(), filter()
1. 공립이면서 교육방식이 전문인 데이터를 출력하시오
2. 사립이면서 교육방식이 표준인 데이터를 출력하시오
3. 학교 이름이 V로 시작하는 데이터를 출력하시오
4. 학교 이름이 M로 끝나는 데이터를 출력하시오
5. 학교 이름에 NKY가 포함된 데이터를 출력하시오
6. 반의 학생 수가 15명 이상 24명 이하인 데이터를 출력하시오
7. 학교 이름이 입력되지 않은 데이터들을 출력하시오

```python
class_df.printSchema()

print('공립이면서 교육방식이 전문인 데이터를 출력하시오.')
class_df.select('*') \
            .where((class_df.school_type == 'Public') & (class_df.teaching_type == 'Experimental')) \
.show(3)

print('사립이면서 교육방식이 표준 데이터를 출력하시오.')
class_df.select('*') \
            .where((class_df.school_type == 'Non-public') & (class_df.teaching_type == 'Standard')) \
.show(3)

print('학교 이름이 V로 시작하는 데이터를 출력하시오.')
class_df.select('*') \
            .where(class_df.school.like ('V%')) \
.show(3)

print('학교 이름이 M로 끝하는 데이터를 출력하시오.')
class_df.select('*') \
            .where(class_df.school.like ('%M')) \
.show(3)

print('학교 이름에 NKY가 들어가는 데이터를 출력하시오.')
class_df.select('*') \
            .where(class_df.school.like ('%NKY%')) \
.show(3)

print('반의 학생 수가 15명 이상 24명 이하인 데이터를 출력하시오.')
class_df.select('*') \
            .where(class_df.class_std_cnt.between (15, 24)) \
.show(3)

print('학교 이름이 입력되지 않은 데이터들을 출력하시오')
class_df.select('*') \
            .where(class_df.school.isNull()) \
.show(3)

'''
root
 |-- class_cd: string (nullable = true)
 |-- school: string (nullable = true)
 |-- class_std_cnt: string (nullable = true)
 |-- loc: string (nullable = true)
 |-- school_type: string (nullable = true)
 |-- teaching_type: string (nullable = true)

공립이면서 교육방식이 전문인 데이터를 출력하시오.
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     X6Z| CUQAM|           24|   Urban|     Public| Experimental|
|     PW5| DNQDD|           20|Suburban|     Public| Experimental|
|     ROP| DNQDD|           28|Suburban|     Public| Experimental|
+--------+------+-------------+--------+-----------+-------------+

사립이면서 교육방식이 표준 데이터를 출력하시오.
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     6OL| ANKYI|           20|   Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|   Urban| Non-public|     Standard|
|     PGK| CCAAW|           21|Suburban| Non-public|     Standard|
+--------+------+-------------+--------+-----------+-------------+

학교 이름이 V로 시작하는 데이터를 출력하시오.
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     CD8| VHDHF|           20|Rural| Non-public| Experimental|
|     J6X| VHDHF|           16|Rural| Non-public|     Standard|
|     KR1| VHDHF|           15|Rural| Non-public| Experimental|
+--------+------+-------------+-----+-----------+-------------+

학교 이름이 M로 끝하는 데이터를 출력하시오.
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     1Q1| CUQAM|           28|Urban|     Public|     Standard|
|     BFY| CUQAM|           27|Urban|     Public|     Standard|
|     OMI| CUQAM|           28|Urban|     Public|     Standard|
+--------+------+-------------+-----+-----------+-------------+

학교 이름에 NKY가 들어가는 데이터를 출력하시오.
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     6OL| ANKYI|           20|Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|Urban| Non-public|     Standard|
+--------+------+-------------+-----+-----------+-------------+

반의 학생 수가 15명 이상 24명 이하인 데이터를 출력하시오.
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     6OL| ANKYI|           20|   Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|   Urban| Non-public|     Standard|
|     2B1| CCAAW|           18|Suburban| Non-public| Experimental|
+--------+------+-------------+--------+-----------+-------------+

학교 이름이 입력되지 않은 데이터들을 출력하시오
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     MDS|  null|           18|Rural| Non-public|     Standard|
|     MDE|  null|           10|Rural| Non-public| Experimental|
|     6PP|  null|         null| null|       null|         null|
+--------+------+-------------+-----+-----------+-------------+
'''
```

### Where, Filter - SQL
```python
class_df.printSchema()

print('공립이면서 교육방식이 전문인 데이터를 출력하시오.')
spark.sql('''
    select * from class where school_type = 'Public' and teaching_type = 'Experimental'
''').show(3)

print('사립이면서 교육방식이 표준 데이터를 출력하시오.')
spark.sql('''
    select * from class where school_type = 'Non-public' and teaching_type = 'Standard'
''').show(5)

print('학교 이름이 V로 시작하는 데이터를 출력하시오.')
spark.sql('''
    select * from class where school like 'V%'
''').show(3)

print('학교 이름이 M로 끝하는 데이터를 출력하시오.')
spark.sql('''
    select * from class where school like '%M'
''').show(3)

print('학교 이름에 NKY가 들어가는 데이터를 출력하시오.')
spark.sql('''
    select * from class where school like '%NKY%'
''').show(3)

print('반의 학생 수가 15명 이상 24명 이하인 데이터를 출력하시오.')
spark.sql('''
    select * from class where class_std_cnt between 15 and 24
''').show(3)

print('학교 이름이 입력되지 않은 데이터들을 출력하시오')
spark.sql('''
    select * from class where school is null
''').show(3)

'''
root
 |-- class_cd: string (nullable = true)
 |-- school: string (nullable = true)
 |-- class_std_cnt: string (nullable = true)
 |-- loc: string (nullable = true)
 |-- school_type: string (nullable = true)
 |-- teaching_type: string (nullable = true)

공립이면서 교육방식이 전문인 데이터를 출력하시오.
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     X6Z| CUQAM|           24|   Urban|     Public| Experimental|
|     PW5| DNQDD|           20|Suburban|     Public| Experimental|
|     ROP| DNQDD|           28|Suburban|     Public| Experimental|
+--------+------+-------------+--------+-----------+-------------+

사립이면서 교육방식이 표준 데이터를 출력하시오.
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     6OL| ANKYI|           20|   Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|   Urban| Non-public|     Standard|
|     PGK| CCAAW|           21|Suburban| Non-public|     Standard|
|     UWK| CCAAW|           19|Suburban| Non-public|     Standard|
|     A33| CIMBB|           19|   Urban| Non-public|     Standard|
+--------+------+-------------+--------+-----------+-------------+

학교 이름이 V로 시작하는 데이터를 출력하시오.
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     CD8| VHDHF|           20|Rural| Non-public| Experimental|
|     J6X| VHDHF|           16|Rural| Non-public|     Standard|
|     KR1| VHDHF|           15|Rural| Non-public| Experimental|
+--------+------+-------------+-----+-----------+-------------+

학교 이름이 M로 끝하는 데이터를 출력하시오.
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     1Q1| CUQAM|           28|Urban|     Public|     Standard|
|     BFY| CUQAM|           27|Urban|     Public|     Standard|
|     OMI| CUQAM|           28|Urban|     Public|     Standard|
+--------+------+-------------+-----+-----------+-------------+

학교 이름에 NKY가 들어가는 데이터를 출력하시오.
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     6OL| ANKYI|           20|Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|Urban| Non-public|     Standard|
+--------+------+-------------+-----+-----------+-------------+

반의 학생 수가 15명 이상 24명 이하인 데이터를 출력하시오.
+--------+------+-------------+--------+-----------+-------------+
|class_cd|school|class_std_cnt|     loc|school_type|teaching_type|
+--------+------+-------------+--------+-----------+-------------+
|     6OL| ANKYI|           20|   Urban| Non-public|     Standard|
|     ZNS| ANKYI|           21|   Urban| Non-public|     Standard|
|     2B1| CCAAW|           18|Suburban| Non-public| Experimental|
+--------+------+-------------+--------+-----------+-------------+

학교 이름이 입력되지 않은 데이터들을 출력하시오
+--------+------+-------------+-----+-----------+-------------+
|class_cd|school|class_std_cnt|  loc|school_type|teaching_type|
+--------+------+-------------+-----+-----------+-------------+
|     MDS|  null|           18|Rural| Non-public|     Standard|
|     MDE|  null|           10|Rural| Non-public| Experimental|
|     6PP|  null|         null| null|       null|         null|
+--------+------+-------------+-----+-----------+-------------+
'''
```