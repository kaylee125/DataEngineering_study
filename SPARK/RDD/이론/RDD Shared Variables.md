# Shared Variables
- 모든 노드에서 사용하기 위한 공유변수
- 공유변수로 지정한 값은 모든 노드에 중복되어 캐시된다.
- 반복적으로 사용해야하는 변수라면,  
  스파크의 노드는 네트워크를 통해 통신 하기 때문에 모든 노드에 중복 캐시하는 시스템적 비용보다  
  네트워크 과정에서 발생하는 오버헤드 비용이 더 많이 발생하게 된다.
- Spark는 분산 컴퓨팅을 위한 클러스터 환경에서 작업을 처리하며, 이 때 공유 변수는 여러 태스크 간에 값을 공유하고 동기화하는 데 사용한다.
- 공유 변수는 Spark의 분산 환경에서 데이터 공유와 동기화를 효율적으로 수행하기 위해 사용됩니다. 이를 통해 불필요한 데이터 복사 및 네트워크 트래픽을 줄이고, 태스크 간의 작업을 조율할 수 있습니다.

## 1.Broadcast Variables
- 각 노드에 공유되는 읽기 전용 변수
-  클러스터의 모든 태스크가 동일한 참조 데이터를 사용해야 하는 경우에 브로드캐스트 변수를 활용
```python
# 학생별 수업 카테고리 코드로 지정되어 있는 값을, 카테고리의 상세 값으로 변경한다고 가정
data = [("홍길동","DE"),
    ("이제동","DS"),
    ("하명도","DE"),
    ("변현재","WD")]

code_desc = {"DE":"Data Engineer", "DS":"Data Science", "WD":"Web Developer"}

# sparkContext.broadcast(e)

students_rdd = sc.parallelize(data)
students_rdd.mapValues(lambda e : code_desc[e]).collect()

'''
[('홍길동', 'Data Engineer'),
 ('이제동', 'Data Science'),
 ('하명도', 'Data Engineer'),
 ('변현재', 'Web Developer')]
'''

# BroadCast_variables 사용하기
broadcast_code = sc.broadcast(code_desc)
broadcast_code.value
# 공유변수에서 DE 키값을 삭제하더라도
del(broadcast_code.value['DE'])
# 코드 변환이 성공적으로 이루어진다. 즉 삭제가 되지 않음
# broadcast 함수를 사용해 생성하는 시점에 SparkContext 사용하기 때문
students_rdd.mapValues(lambda e : broadcast_code.value[e]).collect()

'''
[('홍길동', 'Data Engineer'),
 ('이제동', 'Data Science'),
 ('하명도', 'Data Engineer'),
 ('변현재', 'Web Developer')]
'''
```

## 2.Accumulator
- 각 노드에 공유되는 누산기 함수
- 여러 태스크에서 작업을 수행하는 동안 값을 추가 또는 집계하기 위한 변수

```python
accum = sc.accumulator(0)
def change_cate(e):
    accum.add(1)
    return broadcast_code.value[e]

students_rdd.mapValues(lambda e : change_cate(e)).collect()
accum.value

'''
4
'''
```