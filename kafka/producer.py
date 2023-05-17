from kafka import KafkaProducer
import csv

# Kafka broker 서버의 호스트와 포트
bootstrap_servers = 'kafka01:9092,kafka02:9092,kafka03:9092'

# Kafka 토픽명
topic_name = 'netflix-credit'

# CSV 파일 경로
csv_file_path = '/usr/local/data/credits.csv'

# Kafka 프로듀서 설정
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# CSV 파일 읽어서 Kafka에 전송
with open(csv_file_path, 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        # CSV 파일의 각 row를 문자열로 변환하여 Kafka 토픽에 전송
        message = ','.join(row).encode('utf-8')
        producer.send(topic_name, message)
        print(f'Sent message: {message}')

# Kafka 프로듀서 종료
producer.close()
