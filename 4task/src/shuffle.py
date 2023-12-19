import pika
from sys import argv
import json

dir_num = argv[1]
reducers_amount = int(argv[2])

exit_code = "-1"
alphabet = "abcdefghijklmnopqrstuvwxyz"
letters_per_reducer = (len(alphabet) // reducers_amount)


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="exchange", exchange_type="direct")
queue = f"mapper-shuffle{dir_num}"
channel.queue_declare(queue=queue, auto_delete=True)
channel.queue_bind(exchange="exchange", queue=queue)

for i in range(reducers_amount):
    queue = f"shufflers-reduce{str(i + 1)}"
    channel.queue_declare(queue=queue, auto_delete=True)
    channel.queue_bind(exchange="exchange", queue=queue)


words_dict = {}

def handle_mapper_response(channel, method, properties, body):
    message = json.loads(body.decode())
    key = message[0]
    if key == exit_code:
        channel.stop_consuming()
        # Отправка reducer'ам
        key_values = list(words_dict.items())
        for key_value in key_values:
            key_letter = key_value[0][0]
            # В зависимости от первой буквы ключа, отправляем определенному reducer'у
            queue = f"shufflers-reduce{str((alphabet.find(key_letter) // letters_per_reducer) % reducers_amount + 1)}"
            channel.basic_publish(exchange="exchange", routing_key=queue, body=json.dumps(key_value))
        for i in range(reducers_amount):
            queue = f"shufflers-reduce{str(i + 1)}"
            channel.basic_publish(exchange="exchange", routing_key=queue, body=json.dumps([exit_code, [1]]))
        print(f"Shuffler{dir_num}: done")
        return
    if key not in words_dict.keys():
        words_dict[key] = [1]
    else:
        words_dict[key].append(1)
    
queue = f"mapper-shuffle{dir_num}"
channel.basic_consume(queue, handle_mapper_response, auto_ack=True)
channel.start_consuming()

