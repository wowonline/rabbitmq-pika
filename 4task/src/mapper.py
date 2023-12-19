import pika
from sys import argv
import json

dir_num = argv[1]

exit_code = "-1"


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="exchange", exchange_type="direct")
queue = f"splitter_mapper{dir_num}"
channel.queue_declare(queue=queue, auto_delete=True)
channel.queue_bind(exchange="exchange", queue=queue)

queue = f"mapper-shuffle{dir_num}"
channel.queue_declare(queue=queue, auto_delete=True)
channel.queue_bind(exchange="exchange", queue=queue)


def handle_splitter_response(channel, method, properties, body):
    message = json.loads(body.decode())
    queue = f"mapper-shuffle{dir_num}"
    if message == exit_code:
        channel.stop_consuming()
        channel.basic_publish(exchange="exchange", routing_key=queue, body=json.dumps((exit_code, 1)))
        print(f"Mapper{dir_num}: done")
        return
    words_list = message.split()
    for word in words_list:
        channel.basic_publish(exchange="exchange", routing_key=queue, body=json.dumps((word, 1)))


queue = f"splitter_mapper{dir_num}"
channel.basic_consume(queue, handle_splitter_response, auto_ack=True)
channel.start_consuming()
