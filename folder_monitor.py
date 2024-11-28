import os
import time
import pika
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configurações do RabbitMQ
RABBITMQ_HOST = 'localhost'
LASER_QUEUE = 'laser_notifications'
FACAS_QUEUE = 'facas_notifications'

# Diretórios a serem monitorados
LASER_DIR = r"D:\Laser"
FACAS_DIR = r"D:\Laser\FACAS OK"

# Configurar conexão com RabbitMQ
def send_to_queue(queue_name, message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    connection.close()

# Handler de eventos de arquivo
class FolderEventHandler(FileSystemEventHandler):
    def __init__(self, queue_name):
        self.queue_name = queue_name

    def on_created(self, event):
        if not event.is_directory:
            file_name = os.path.basename(event.src_path)
            print(f"Novo arquivo detectado: {file_name} em {self.queue_name}")
            send_to_queue(self.queue_name, file_name)

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

