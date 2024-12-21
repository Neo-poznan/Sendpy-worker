import smtplib
import logging
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logging.basicConfig(level=logging.INFO, filename='log.log', filemode='w')


def create_smtp_server_and_login(sender_email, app_password):
    '''
    Создаем SMTP сервер и логинимся на нем
    '''
    try:
        if '@gmail.com' in sender_email:
            server = smtplib.SMTP(host='smtp.gmail.com', port=587)
        elif '@yandex.ru' in sender_email:
            server = smtplib.SMTP(host='smtp.yandex.ru', port=587)
        server.starttls()
        server.login(sender_email, app_password)
    except Exception as e:
        logging.error(f'{[datetime.datetime.now()]} Error creating smtp server: {e}')
    return server

def send_email(server, sender_email, recipients_email, message):
    '''
    Формируем сообщение указываем заголовок от кого и кому 
    и передаем текст сообщения как html 
    отправляем сообщение и выключаем сервер 
    '''
    msg = MIMEMultipart('alternative')
    msg['Subject'] = message
    msg['From'] = sender_email
    msg['To'] = recipients_email
    html_part = MIMEText(message, 'html')
    msg.attach(html_part)
    try:
        server.sendmail(sender_email, recipients_email, msg.as_string())
        server.quit()
    except Exception as e:
        logging.error(f'{[datetime.datetime.now()]} Error sending email: {e}')

