from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
#from elasticsearch.connection import RequestsHttpConnection
from requests_aws4auth import AWS4Auth
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
##################################

# AWS Elasticsearch 호스트 정보
host = 'search-netflix-es-ns672rvwguyves2c6fch72ryqq.ap-northeast-2.es.amazonaws.com'
region = 'ap-northeast-2'

# AWS 인증 정보
aws_access_key = 'AKIAXWZU627FGHR5TZLI'
aws_secret_access_key = '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'

#aws_session_token = '<aws_session_token>'

# AWS4Auth 객체 생성
aws_auth = AWS4Auth(
    aws_access_key,
    aws_secret_access_key,
    region,
    'es',
    #session_token=aws_session_token
)

# Elasticsearch 클라이언트 객체 생성
es = Elasticsearch(
    hosts=[{'host': host, 'port': 443,'scheme':'http'}],
    http_auth==aws_auth,
    #use_ssl=True,
    verify_certs=True,
   # connection_class=RequestsHttpConnection
)

# 새로운 인덱스 생성
es.indices.create(index='netflix-credits', ignore=400)


# Kafka topic 데이터를 Elasticsearch로 전송
for msg in consumer:
    data = msg.value.decode('utf-8')  # 데이터 디코딩
    # Elasticsearch에 데이터 적재
    es.index(index='netflix-credits', document=json.dumps(data))
