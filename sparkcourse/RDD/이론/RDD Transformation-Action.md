# Transformation - Action

## RDD Transformation
-  데이터를 가공하기 위한 논리적 실행계획
-  기존의 RDD에 연산이 반영된 새로운 RDD를 반환한다.

## Transformation 메서드

1. filter()
2. map()
3. flatMap()
4. distinct()
5. zip()
6. join()
6. reduceByKey()
7. mapValues()
8. sortBy()

### Transformation - filter()
```python
score_rdd = sc.textFile('/rdd/score.txt')

# filter()
# 조건에 맞는 데이터만
score_rdd.filter(lambda e : '스파크' in e).filter(lambda e : '홍길' in e).collect()
type(score_rdd.filter(lambda e : '스파크' in e))
```

### Transformation - map()
```python
# map()
# 각각의 요소에 적용
data = [1, 3, 4, 7, 9]
map_rdd = sc.parallelize(data)
map_rdd.map(lambda e : e * 2).collect()
```

### Transformation - flatMap()
```python
# FlatMap()
# 각각의 요소에 함수를 적용한 다음 평면화 시켜주는 함수
nlist = [[1, 2, 3], 
         [4, 5, 6], 
         [7, 8, 9]]

flat_rdd = sc.parallelize(nlist)

def append_data(e) :
    e.append(100)
    return e

res = flat_rdd.flatMap(lambda e : append_data(e)).collect()
len(res)
# [[1, 2, 3, 100], [4, 5, 6, 100], [7, 8, 9, 100]]
res
```

### Transformation - distinct()
```python
# 중복 제거
nlist = [1, 2, 3, 1, 2, 3, 1, 2, 3 , 4, 5]
distinct_rdd = sc.parallelize(nlist)
distinct_rdd.distinct().collect()

slist = ['안녕','반가워','hello','hello']
distinct_rdd2 = sc.parallelize(slist)
distinct_rdd2.distinct().collect()
```

### Transformation - zip()
```python
# zip : 두 RDD를 결합해 key - value 형태의 RDD(Pair RDD)로 생성
foods = ['파스타','스테이크', '불고기', '비빔밥', '김치']
category = ['양식', '양식', '한식', '한식', '한식']

foods_rdd = sc.parallelize(foods)
category_rdd = sc.parallelize(category)

zip_rdd = category_rdd.zip(foods_rdd)
zip_rdd
zip_rdd.collect()

# 2
tmp = list(map(lambda a, b : (a, b), category, foods))
tmp_rdd = sc.parallelize(tmp)
tmp_rdd.collect()

tmp = list((zip(category, foods)))
tmp
```

### Transformation - reduceByKey(), mapValues()
```python
# redusceByKey : 키 값을 기준으로  value 값들을 연산
# mapValues : Pair RDD의 value들에 대해 map 연산을 수행
res = zip_rdd.reduceByKey(lambda a, b : a + ',' + b).mapValues(lambda a : a.split(','))
res.collect()
```

### Transformation - sortBy()
```python
# 키값으로 내림차순
res.sortBy(lambda e : e[0], ascending = False).collect()

# value 값으로 오름차순
# 메뉴 가짓 수로 오름차순
res.sortBy(lambda e : len(e[1][1]), ascending = False).collect()
```

#### 연습문제 : distinct, zip, reduceByKey, sortBy 
- hdfs의 /score.txt 파일을 읽어와 RDD로 생성하시오
- 각 과목별 명단을 추출하시오
- 각 과목별 평균점수를 추출하시오
- 이때 중복으로 들어간 홍진호의 데이터는 한번만 적용되도록 합니다.  

- 결과 :  
[('스파크', {'명단': ['하명도', '홍길동', '임꺽정']}), ('텐서플로우', {'명단': ['임요환', '홍진호', '이윤열']})]       
 
[('스파크', {'평균점수': 63.333333333333336}), ('텐서플로우', {'평균점수': 70.66666666666667})]

```python
score_rdd = sc.textFile('/rdd/score.txt')
base = score_rdd.distinct().map(lambda e : e.split(' '))
base.collect()

student = base.map(lambda e : (e[1], [e[0]])).reduceByKey(lambda a, b : a + b).mapValues(lambda a : {'명단: ': a})
student.collect()

score_avg = base.map(lambda e : (e[1], [int(e[2])])).reduceByKey(lambda a, b : a + b).mapValues(lambda a : {'평균점수': sum(a) / len(a)})
score_avg.collect()
```

### Transformation - join()
- 키를 기준으로 두 RDD를 결합

```python
res = student.join(score_avg)
res.collect()

# [('스파크', {'명단': ['임꺽정', '하명도', '홍길동'], '평균점수': 63.333333333333336}),
# ('텐서플로우', {'명단': ['이윤열', '홍진호', '임요환'], '평균점수': 70.66666666666667})]

re = student.join(score_avg).mapValues(lambda e : dict(e[0], 평균점수 = e[1]['평균점수']))
re.collect()
# [('스파크', {'명단: ': ['하명도', '홍길동', '임꺽정'], '평균점수': 63.333333333333336}),
# ('텐서플로우', {'명단: ': ['홍진호', '임요환', '이윤열'], '평균점수': 70.66666666666667})]
```

## Spark Shuffle
- 스파크에서 연산은 단일 파티션에서 작동   
- reduceByKey와 같이 특정 키에 매핑된 모든 값에 대한 연산을 수행하기 위해서는 파티션에 흩어진 특정키에 해당하는 값을 하나의 파티션으로 모아 줄 필요가 있음  
- 모든 키에 대한 모든 값을 찾기 위해 모든 파티션을 탐색하고, 해당하는 값들을 하나의 파티션으로 옮겨오는 과정을 셔플이라고 부른다.  
- 디스크 IO 또는 네트워크 IO가 발생함으로 비용이 매우 비싼 작업 <br>

ex)   
- filter : 각 파티션에 있는 하나의 튜플에 대해 조건을 탐색하면 됨으로 셔플 발생 x  
- reduceByKey : 연산을 시작하기 위해서는 우선적으로 모든 파티션에 분산되어 있는 특정 키 값을 수집해야함으로 셔플 발생 
- 셔플이 발생하는 함수들 :
 - subtractByKey
 - groupBy
 - foldByKey
 - reduceByKey
 - aggregateByKey
 - transformations of a join of any type
 - distinct
 - cogroup

## RDD Action
- transformation 연산을 통해 생성한 논리적 실행계획을 최적화 하여 연산을 수행. 빠른 연산이 가능

### Action Method
1. collect()
2. take()
3. takeOrdered()
4. top()
5. countByValue()
6. foreach()
7. reduce()
8. saveAsTextFile()
9. max()
10. min()
11. mean()
12. variance()
13. stdev()
14. stats()

### Action - collect()
- RDD에 있는 데이터를 리스트로 반환해주는 함수

```python
score_rdd = sc.textFile('/rdd/score.txt')
score_rdd.collect()

'''
['하명도 스파크 50',
 '홍길동 스파크 80',
 '임꺽정 스파크 60',
 '임요환 텐서플로우 100',
 '홍진호 텐서플로우 22',
 '홍진호 텐서플로우 22',
 '이윤열 텐서플로우 90']
 '''
```

### Action - take()
- RDD의 첫 번째 요소를 반환해주는 함수

```python
data = [1, 2, 3, 4, 5]
take_rdd = sc.parallelize(data)
take_rdd.take(3)

# [1, 2, 3]
```

### Action - takeOrdered()
- ascending 혹은 키 값을 기준으로 정렬하여 N개의 요소들을 반환해주는 함수

```python
data = [1, 20, 32, 400, 51, 100, 0.1]
to_rdd = sc.parallelize(data)
to_rdd.takeOrdered(4)
to_rdd.takeOrdered(4, lambda e : -e)

'''
[0.1, 1, 20, 32]
[400, 100, 51, 32]
'''
```

### Action - top()
- 내림차순으로 N개의 요소를 반환해주는 함수

```python
data = [1, 20, 32, 400, 51, 100, 0.1, 'a']
to_rdd = sc.parallelize(data)
to_rdd.top(7, key = str)

# ['a', 51, 400, 32, 20, 100, 1]
```

### Action - countByValues()
- value의 갯수를 count 해주는 함수

```python
chars_rdd = sc.parallelize(['a', 'a', 'a', 'b', 'b', 'b', 'b', 'c', 'c', 'de', 'de'])
chars_rdd.countByValue()

# defaultdict(int, {'a': 3, 'b': 4, 'c': 2, 'de': 2})
```

### Action - reduce()
- 요소 별 연산 수행

```python
nums = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
nums.reduce(lambda a, b : a + b)

# 55
```

### Action - foreach()
- RDD의 모든 요소에 function을 적용하는 함수

```python
nums = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def f(x): print(x)
nums = nums.foreach(f)
nums.collect()

# 현재 docs 예제 동작 불가
```

### Action - saveAsTextFile()
- RDD를 파일로 저장

```python
re.saveAsTextFile('/rdd/score_transform.txt')
```

### Action - max, min, mean, variance, stdev, stats
```python
nums = sc.parallelize([1, 4, 2, 3, 7, 6, 5])
nums.max()
nums.min()
nums.mean()
nums.variance()
nums.stdev()
nums.stats()

'''
7
1
4.0
4.0
2.0
(count: 7, mean: 4.0, stdev: 2.0, max: 7, min: 1)
'''
```