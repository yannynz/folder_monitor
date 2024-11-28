import os
import time
import pika
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import json

# Configurações do RabbitMQ
RABBITMQ_HOST = '192.168.10.28'  # Corrigido: remova as aspas extras e a vírgula
RABBITMQ_PORT = 5672
RABBITMQ_VHOST = '/'
RABBITMQ_USER = 'guest'
RABBITMQ_PASSWORD = 'guest'

LASER_QUEUE = 'laser_notifications'
FACAS_QUEUE = 'facas_notifications'

# Diretórios a serem monitorados
LASER_DIR = r"D:\Laser"
FACAS_DIR = r"D:\Laser\FACAS OK"

# Função para enviar mensagem para a fila RabbitMQ
def send_to_queue(queue_name, message):
    try:
        # Corrigido: configurando a conexão com os parâmetros corretos
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials
            )
        )
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)  # Garantir persistência da fila
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensagem persistente
            )
        )
        connection.close()
    except Exception as e:
        print(f"Erro ao enviar mensagem para a fila {queue_name}: {str(e)}")

# Handler de eventos de arquivo
class FolderEventHandler(FileSystemEventHandler):
    def __init__(self, queue_name):
        self.queue_name = queue_name

    def on_created(self, event):
        if not event.is_directory:
            file_name = os.path.basename(event.src_path)
            file_info = {
                "file_name": file_name,
                "path": event.src_path,
                "timestamp": time.time()
            }
            print(f"Novo arquivo detectado: {file_name} em {self.queue_name}")
            send_to_queue(self.queue_name, file_info)

# Configurar monitoramento de pastas
def monitor_folder(folder_path, queue_name):
    event_handler = FolderEventHandler(queue_name)
    observer = Observer()
    observer.schedule(event_handler, folder_path, recursive=False)
    observer.start()
    print(f"Monitorando {folder_path} e enviando para {queue_name}")
    return observer

if __name__ == "__main__":
    laser_observer = monitor_folder(LASER_DIR, LASER_QUEUE)
    facas_observer = monitor_folder(FACAS_DIR, FACAS_QUEUE)
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        laser_observer.stop()
        facas_observer.stop()
    laser_observer.join()
    facas_observer.join()

