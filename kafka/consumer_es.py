from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

# Kafka Consumer 설정
consumer = KafkaConsumer(
    'netflix-credit',  # Kafka 토픽 이름
    bootstrap_servers='kafka01:9092,kafka02:9092,kafka03:9092',
    # bootstrap_servers=['kafka01:9092,kafka02:9092,kafka03:9092'],  # Kafka Broker 서버 >목록
    auto_offset_reset='earliest',  # 데이터 시작 오프셋 설정 (가장 이른 데이터부터 읽기)
    enable_auto_commit=True,  # 오프셋 자동 커밋 설정
   # group_id='netflix-consumer-group',  # Kafka Consumer 그룹 ID
   # value_deserializer=lambda x: x.decode('utf-8')  # 데이터 디시리얼라이저 설정 (UTF-8 >인코딩)
)
# Elasticsearch 클라이언트 설정
es = Elasticsearch(
        hosts=[{'host':'search-netflix-es-ns672rvwguyves2c6fch72ryqq.ap-northeast-2.es.amazonaws.com:','port':443,"scheme": "http"}],  # AWS OpenSearch 클라우드 ID
    basic_auth=('admin', '!@12Qwaszx!!'),  # AWS OpenSearch 접속에 사용되는 인증 정보
    verify_certs=True
    )

# Kafka topic 데이터를 Elasticsearch로 전송
for msg in consumer:
    data = msg.value.decode('utf-8')  # 데이터 디코딩
   # print(f'Received message: {msg.value.decode("utf-8")}')
   # print(json.dumps(data))
    # Elasticsearch에 데이터 적재
    es.index(index='netflix-client', document=json.dumps(data))