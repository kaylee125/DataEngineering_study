from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from requests_aws4auth import AWS4Auth
import boto3
import json
from kafka import KafkaConsumer

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

host = 'search-netflix-es-ns672rvwguyves2c6fch72ryqq.ap-northeast-2.es.amazonaws.com' # cluster endpoint, for example: my-test-domain.us-east-1.es.amazonaws.com
region = 'ap-northeast-2' # e.g. us-west-1

credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region)
index_name = 'netflix-credit'

client = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

# 새로운 인덱스 생성
client.indices.create(index='netflix-credits', ignore=400)


# Kafka topic 데이터를 Elasticsearch로 전송
for msg in consumer:
    data = msg.value.decode('utf-8')  # 데이터 디코딩
    # Elasticsearch에 데이터 적재
    client.index(index='netflix-credits', body=json.dumps(data))