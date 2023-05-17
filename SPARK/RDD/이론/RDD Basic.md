# Spark 데이터 종류
- RDD : Low-level API에 해당 하는 데이터, 스파크에서 동작하는 모든 데이터는 결국 RDD형태로 연산된다  
- DataFrame : 열과 행으로 이루어진 테이블 형태의 데이터
- DataSet : 한가지 타입의 값만 저장할 수 있는 DataFrame. PySpark에서는 사용하지 않음

## RDD 

### Resillient Distributed Data
- Resillient : 회복력 있는  
- Distributed : 분산  
- Data : 데이터  
    * Resilient : 장애가 발생할 경우 자동으로 데이터를 복구
    * Distributed : 데이터를 읽고 쓸 때 데이터를 파티셔닝 하여 병렬로 읽고 쓴다. 직렬로 모두 읽을 때 보다 속도가 빠르다
 
 
### RDD의 특징
- Read Only : 변경불가능 객체
- Lazy Evaluation : 늦은 수행

### RDD 파티셔닝
- 커다란 RDD를 처음 부터 끝까지 직렬로 읽고 쓰면 시간이 오래 걸린다.
- RDD를 작은 부분(Partition) 으로 쪼개 각 Patition 을 병렬로 읽고 써 실행속도를 향상시킬 수 있다.
- Patition을 나눠줄때는 스파크 클러스터 환경의 cpu core 숫자에 맞춰주는 것이 속도 측면에서 유리하다.

### RDD - 생성
```python
# RDD로 만들 데이터, 파티셔닝 수
distData = sc.parallelize(data, 5)

# RDD 확인

# collect : RDD에 있는 데이터를 list로 반환하는 함수
distData.collect()

# RDD 파티셔닝 개수 확인
distData.getNumPartitions()

# RDD로 만들 text file Load
score_rdd = sc.textFile('/rdd/score.txt', 20)
```

