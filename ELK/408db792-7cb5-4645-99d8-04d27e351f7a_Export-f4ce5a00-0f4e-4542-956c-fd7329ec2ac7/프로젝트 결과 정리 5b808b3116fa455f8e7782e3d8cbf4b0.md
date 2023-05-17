# 프로젝트 결과 정리

# 프로젝트 세부 내용

### 프로젝트명: kafka를 활용한 실시간 데이터 처리를 위한 데이터파이프라인 구축 및 시각화

### 프로젝트 기간: 2023.04.04~2023.04.14

### 기술스택: AWS EC2,Kafka,Logstash,AWS opensearch service,python

# Overview

본 프로젝트에서는 aws 클라우드 환경에서 kafka 클러스터를 구성하고, consumer에서 ELK스택으로 데이터를 전송 및 처리하는 구조로 구성한 후 opensearch dashboard로 간단한 시각화까지 진행했습니다.

해당 프로젝트의 테스트는 csv 데이터셋으로 진행했으나, 전체적인 파이프라인은 실시간 데이터 파이프라인 구축을 위해 만들어졌습니다. Broker을 3대의 서버로 구성했기 때문에  대량의 실시간 데이터를 처리할때 빠른 데이터 처리 및 전송이 가능하고 장애 발생 시에도 데이터 손실을 방지할 수 있습니다.

# Dataset

kaggle에서 제공하는 **Netflix Movies and TV Shows**  csv파일 형태의 데이터셋을 사용했습니다.

csv 데이터셋을 row 단위별로 kafka에 전송하여 Kafka가 실시간 데이터 스트리밍 플랫폼으로 동작함으로써  마치 각 row에 해당하는 데이터를 실시간 데이터인 것 처럼 처리하였습니다.

[Netflix Movies and TV Shows](https://www.kaggle.com/datasets/dgoenrique/netflix-movies-and-tv-shows)

# Objective

이번 프로젝트를 통해  opensearch dashboard을 통해 데이터 분석 및 시각화를 하여 아래 내용들에 대해 확인해보았습니다.

## [Netflix 데이터셋을 이용한 데이터 분석 및 시각화]

국가별 컨텐츠 발행 수 비교
[전체국가] 연도별 영상 유형에 따른 업로드 추이 비교
[한국] 연도별(2017-2022) 영상 유형에 따른 업로드 추이 비교
[미국] 연도별 영상 유형에 따른 업로드 추이 비교
전체국가 가장 많은 장르 종류 Top3
발행 연도별 Imdb Imdb 사이트 점수 비교
전체국가 연재된 최종시즌 분포

# Data Architecture

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled.png)

## kafka를 선택한 이유

Kafka는 고가용성을 잘 지원해주기 때문에 실시간 데이터 처리 파이프라인 구성에 있어 매우 중요하고 필요한 서비스입니다.
kafka는 복수의 broker을 생성할 수 있으며 replication-factor 옵션을 통해 각 broker에 leader와 follower 역할을 나누어 데이터를 복제해 관리하며 고가용성을 보장합니다.
kafka를 빈 서버에서부터 처음부터 구축 및 운영해보며 kafka가 내부적으로 어떻게 동작하는지에 대해 많이 이해할 수 있었고,  본 프로젝트에서 Kafka를 직접 다루어보면서 온프레미스 환경에서 서비스와 데이터 환경에 따라 kafka를 구축하고 관리할 수 있는 작은 토대가 되었습니다.

## ELK 를 선택한 이유

쉽고 빠르게 데이터 수집, 시각화 과정을 진행할 수 있기 때문에 해당 기술 스택을 사용했습니다. 실시간 데이터를 적용한다면 실시간으로 데이터 시각화가 가능하기 때문에 log데이터나 실시간 데이터 적용시 효율적으로 모니터링과 데이터 분석이 가능해집니다.

# 개발내용

## kafka cluster 구성

- kafka cluster 구축을 위한 aws EC2 인스턴스 3대의 서버 생성
    - java,zookeeper,kafka 설치 및 환경설정
- kafka 토픽생성
    - replica(복제본) 2 , partition 3으로 지정하여 각 데이터셋 별로 csv생성

## Kafka Producer

kafka producer > python으로 처리
- csv >json으로 변환 후 kafka topic 전송
- credit_producer.py
- title_producer.py

## Data opensearch로 전송

### 1)logstash config 파일 작성

logstash_title.conf
logstash_credit.conf

### 2)데이터 시각화

- opensearch Dashboard에서 생성한 Index 바탕으로 시각화

# Data Visualization

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled%201.png)

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled%202.png)

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled%203.png)

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled%204.png)

---

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled%205.png)

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled%206.png)

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled%207.png)

## 전체 대시보드

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled%208.png)

# What I learned

이번 프로젝트를 통해 이론적으로 학습했던 Kafka와 ELK 스택에 대한 내용을 다시 되짚어보고 나아가 실시간 대용량 데이터 처리에 적합한 kafka cluster을 구축 및 운영해보는 좋은 계기가 되었던 것 같습니다.

실제 클러스터를 구축해서 사용해보니 데이터의 양에 따라 테스트해보면서 가장 적절한  partition으로 물리적 세그먼트를 구성해 병렬처리를 하는 것이 빠르고 효율적인 데이터 처리 프로세스를 구성하는데에 있어 중요한 요소라는 것을 깨달았습니다.

본 프로젝트에서 더 나아가 향후 해당 데이터파이프라인에 csv파일이 아닌 실제 실시간 대용량 데이터를 받아 opensearch 혹은 elasticsearch와 같은 검색엔진을 통해 실시간으로 데이터를 모니터링하고 분석하는데에 적용해볼 계획입니다. 또한 consumer group을 두개로 분리하여 데이터 시각화용과 데이터 백업(HDFS or S3)로 나누어 처리될 수 있도록 구성해볼 계획입니다.

# Issue

**1) logstash parsing error**
logstash로 kafka->opensearch로 데이터를 전송하기 위해 plugin config 파일을 작성하는데 지속적인 parsing error 가 났습니다.
이를 해결하기 위해 logstash config파일을 여러번 수정하였으나, 오류가 잘 잡히지 않았습니다.
여러 고민 끝에 kafka producer 단계로 다시 돌아와 데이터 전송방식을 변경하면서 column과 데이터가 불일치하거나 parsing error가 나는 부분들을 다 해결할 수 있었습니다.

**기존방식**
csv파일을 kafka producer에서 row단위로 전송> logstash config 에서 filter을 이용해 column 지정 및 json 변환

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled%209.png)

**변경방식**
csv파일을 Kafka producer에서 row->json으로 변환 후 전송>logstash에서 별다른 데이터 변환없이 바로 opensearch로 전송

![Untitled](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20%E1%84%80%E1%85%A7%E1%86%AF%E1%84%80%E1%85%AA%20%E1%84%8C%E1%85%A5%E1%86%BC%E1%84%85%E1%85%B5%205b808b3116fa455f8e7782e3d8cbf4b0/Untitled%2010.png)