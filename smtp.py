import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def create_smtp_server_and_login(sender_email, app_password):
    '''
    Создаем SMTP сервер и логинимся на нем
    '''
    if '@gmail.com' in sender_email:
        server = smtplib.SMTP(host='smtp.gmail.com', port=587)
    elif '@yandex.ru' in sender_email:
        server = smtplib.SMTP(host='smtp.yandex.ru', port=587)
    server.starttls()
    server.login(sender_email, app_password)
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
    server.sendmail(sender_email, recipients_email, msg.as_string())
    server.quit()

    