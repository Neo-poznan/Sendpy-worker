import json
import logging
import datetime

from confluent_kafka import Consumer, KafkaError, TopicPartition

from smtp import create_smtp_server_and_login, send_email

logging.basicConfig(level=logging.INFO, filename='log.log', filemode='w')

def main():
    '''
    Запускаем цикл ожидания сообщений. 
    Когда получаем сообщение достаем из него данные и отправляем почту
    '''
    # отключаем автоматическую фиксацию смещения на случай если программа упадет 
    # и сообщение не будет отправлено на почту чтобы в таком случае данные не удалились из брокера
    # и при повторном запуске программа могла заново отправить сообщение
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': '0',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # Отключаем автоматическую фиксацию смещения
    }

    consumer = Consumer(conf)
    topic_partition = TopicPartition('sendpy-topic', 0)
    # Подписка на топики
    consumer.assign([topic_partition])

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
            '''
            try:
                smtp_server = create_smtp_server_and_login(received_message['sender'], received_message['app_password'].replace(' ', ' '))
            except Exception as e:
                logging.error(f'{[datetime.datetime.now()]} Error creating smtp server: {e}')

            try:
                send_email(smtp_server, received_message['sender'], received_message['contact'], received_message['message'])
            except Exception as e:
                logging.error(f'{[datetime.datetime.now()]} Error sending email: {e}')
            '''
            consumer.commit(asynchronous=False)

    finally:
        consumer.close()
        
if __name__ == '__main__':
    main()
