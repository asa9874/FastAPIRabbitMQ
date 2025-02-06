from fastapi import FastAPI
import pika
import json

app = FastAPI()

# RabbitMQ 연결 설정
RABBITMQ_HOST = 'localhost'  # Docker에서 실행 중이므로 localhost로 설정
QUEUE_NAME = 'task_queue'

# RabbitMQ 연결 및 채널 생성
def get_rabbitmq_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    # 큐가 없으면 생성 (내구성 설정)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    return channel

# 메시지 전송 엔드포인트
@app.post("/send_message/")
async def send_message(message: str):
    # RabbitMQ 채널 가져오기
    channel = get_rabbitmq_channel()

    # 메시지를 큐에 발행
    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # 메시지 내구성 설정 (큐에 남아 있음)
        )
    )

    return {"message": "Message sent to RabbitMQ", "data": message}

# 메시지 수신 엔드포인트
@app.get("/receive_message/")
async def receive_message():
    # RabbitMQ 채널 가져오기
    channel = get_rabbitmq_channel()

    # 메시지를 큐에서 소비하는 콜백 함수 정의
    def callback(ch, method, properties, body):
        print(f"Received: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # 메시지 수신 대기
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    # 메시지를 계속 받기 위한 무한 대기
    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

