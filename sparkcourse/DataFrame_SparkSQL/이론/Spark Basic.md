# Spark
- 대규모 데이터 처리를 위한 통합 분석 엔진
   - DataSource에 저장한 데이터를 가공 및 분석하기 위한 도구
   - ex) DataSource : local, HDFS, BigQuery, Amazon S3   

## Spark의 특징

- 스파크는 빠르다.  
- 스파크는 개발자 친화적이다  
- 스파크는 Lazy Evaluation을 지원한다.
- 스파크는 강력한 캐시 기능을 제공한다.
- 스파크는 다양한 프로그래밍언어와 Cluster Manager를 지원한다
- 스파크는 다양한 Data Source를 지원한다
- 스파크는 데이터 분석을 위한 다양한 고급 도구들을 제공해준다

## 하둡 MapReduce와 스파크의 차이

- 하둡의 MapReduce는 데이터 Map 함수의 실행이 끝난 후 디스크에 결과를 저장하고, 그걸 reduce 함수가 다시 읽어와야 한다.   
  디스크 IO가 발생해 성능 손실이 크다.

- Spark는 Map함수의 결과를 메모리에 저장하고 reduce 함수로 스트리밍 해준다.  
  디스크IO가 발생하지 않아 속도가 하둡에 비해 10배이상 빠르다.

# SparkContext / SparkSession

## SparkContext 
- Spark 2버전 이전 Spark Cluster에 접근하기 위한 진입점 
- Spark의 Low-level API인 RDD, accumulators, broadcast variables를 생성할 수 있다.
- Spark Shell을 사용할 경우 sc라는 변수명으로 생성된다.

## SparkSession
- Spark 2버전 이후 Spark Cluster에 접근하기 위한 진입점
- SparkContext를 통해 사용할 수 있는 모든 기능을 사용할 수 있다.
- Spark의 Structured-API인 DataFrame과 Dataset, Spark SQL을 생성할 수 있다.
- Spark Shell을 사용할 경우 spark라는 변수명으로 생성된다. 

## 스파크를 왜 사용하는가?
- 빠르다
- hdfs를 지원한다. (질의를 통해 원하는 데이터를 꺼내올 수 있다.)