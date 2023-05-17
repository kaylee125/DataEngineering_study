from kafka import KafkaConsumer

# Kafka Consumer 설정
consumer = KafkaConsumer(
    'netflix-credit',           # 구독할 토픽명
    bootstrap_servers='kafka01:9092,kafka02:9092,kafka03:9092'  # Kafka 브로커의 주소와 포트
    auto_offset_reset='earliest',        # Consumer의 시작 오프셋 설정 (earliest: 가장 이른 오프셋부터, latest: 가장 최신 오프셋부터)
    group_id='netflix-consumer-group'                  # Consumer 그룹의 ID
)

# 메시지 수신 및 처리
for message in consumer:
    print(f'Received message: {message.value.decode("utf-8")}')