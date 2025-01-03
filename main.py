import json
import logging
import datetime

from confluent_kafka import Consumer, KafkaError, TopicPartition

from smtp import connect_to_smtp_server_and_login, send_email
from config import KAFKA_HOST_AND_PORT, TOPIC, PARTITION

logging.basicConfig(level=logging.INFO, filename='log.log', filemode='w')


def create_consumer() -> Consumer:
    '''
    отключаем автоматическую фиксацию смещения на случай если программа упадет 
    и сообщение не будет отправлено на почту чтобы в таком случае данные не удалились из брокера
    и при повторном запуске программа могла заново отправить сообщение
    '''
    conf = {
        'bootstrap.servers': KAFKA_HOST_AND_PORT,
        'group.id': '0',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # Отключаем автоматическую фиксацию смещения
    }
    consumer = Consumer(conf)
    return consumer


def subscribe_to_topic_partition(consumer: Consumer) -> None:
    '''
    Подписываемся на топик и партицию
    '''
    topic_partition = TopicPartition(TOPIC, PARTITION)
    consumer.assign([topic_partition])


def main():
    '''
    Запускаем цикл ожидания сообщений. 
    Когда получаем сообщение достаем из него данные и отправляем почту
    '''
    consumer = create_consumer()
    subscribe_to_topic_partition(consumer)
    try:
        while True:
            msg = consumer.poll(1.0)  # Ожидание сообщения

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f'{[datetime.datetime.now()]} Error from consumer: {msg.error()}')
                    break

            received_message = json.loads(msg.value().decode('utf-8'))
            logging.info(f'{[datetime.datetime.now()]} Received message: {received_message}')    
            smtp_server = connect_to_smtp_server_and_login(received_message['sender'], received_message['app_password'], received_message['server_host'], received_message['server_port'])
            send_email(smtp_server, received_message['sender'], received_message['contact'], received_message['message'], received_message['header'])
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        
if __name__ == '__main__':
    main()
