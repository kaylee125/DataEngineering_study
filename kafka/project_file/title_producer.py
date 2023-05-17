from kafka import KafkaProducer
import csv
import json

# Kafka broker 서버의 호스트와 포트
bootstrap_servers = 'kafka01:9092,kafka02:9092,kafka03:9092'

# Kafka 토픽명
topic_name = 'titles_netflix'

# CSV 파일 경로
csv_file_path = '/usr/local/data/titles.csv'

# Kafka 프로듀서 설정
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# CSV 파일 읽어오기
with open(csv_file_path, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # CSV row를 JSON 형태로 변환
        json_message = json.dumps(row)
        # JSON 형태의 데이터를 Kafka 토픽에 전송
        producer.send(topic_name, json_message.encode('utf-8'))
        print(f'Sent message: {json_message}')

# Kafka 프로듀서 종료
producer.close()