# from kafka import KafkaConsumer
import json
import boto3
import requests
from requests_aws4auth import AWS4Auth

# Kafka Consumer 설정
# consumer = KafkaConsumer(
#     'netflix-credit',
#     bootstrap_servers='kafka01:9092,kafka02:9092,kafka03:9092',
#     value_deserializer=lambda v: json.loads(v.decode('utf-8')))
data={
  "name": "David",
  "age": 40,
  "city": "Tokyo",
  "country": "Japan"
}



aws_access_key = 'AKIAXWZU627FDG3VZA7Q'
aws_secret_access_key = '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'
aws_region = 'ap-northeast-2'
aws_service= 'es'
url = 'https://search-netflix-es-ns672rvwguyves2c6fch72ryqq.ap-northeast-2.es.amazonaws.com/netflix/_doc'

# AWSRequestsAuth 객체 생성
auth = AWS4Auth(
    access_id = 'AKIAXWZU627FDG3VZA7Q',
    secret_key = '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',
    region = 'ap-northeast-2',
    service= 'es'
)


response = requests.post(url, auth=auth, headers={'Content-Type': 'application/json'}, json=data)

# 응답 확인
if response.status_code == 201:
    print("데이터가 성공적으로 저장되었습니다.")
else:
    print(f"오류가 발생했습니다. 응답 코드: {response.status_code}")
    print(response.text)



# # OpenSearch에 데이터 전송
# def send_to_opensearch(data):
#     url = 'https://search-netflix-es-ns672rvwguyves2c6fch72ryqq.ap-northeast-2.es.amazonaws.com/netflix-credit/_doc' # OpenSearch 인덱스 URL
#     headers = {'Content-Type': 'application/json'}

#     try:
#         response = requests.post(url, data=json.dumps(data),headers=headers)
#         response.raise_for_status()
#         print("데이터가 성공적으로 OpenSearch로 전송되었습니다.")
#     except Exception as e:
#         print("데이터 전송에 실패하였습니다: {}".format(e))

# Kafka Consumer를 통해 데이터 수신 및 OpenSearch로 데이터 전송
# for message in consumer:
#     data = message.value
#     print("수신된 데이터: {}".format(data))
#     send_to_opensearch(data)

# print("수신된 데이터: {}".format(data))
# send_to_opensearch(data)