import pika
from sys import argv
import json

dir_num = argv[1]
shufflers_amount = int(argv[2])

exit_code = "-1"


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="exchange", exchange_type="direct")
queue = f"shufflers-reduce{dir_num}"
channel.queue_declare(queue=queue, auto_delete=True)
channel.queue_bind(exchange="exchange", queue=queue)

queue = "reducers-master"
channel.queue_declare(queue=queue, auto_delete=True)
channel.queue_bind(exchange="exchange", queue=queue)


words_dict = {}
shufflers_exited = 0

def handle_shuffle_respond(channel, method, properties, body):
    global shufflers_exited
    message = json.loads(body.decode())
    key = message[0]
    value = message[1]
    if key == exit_code:
        shufflers_exited += 1
        if shufflers_exited == shufflers_amount:
            channel.stop_consuming()
            words_counts = list(words_dict.items())
            for word_count in words_counts:
                channel.basic_publish(exchange="exchange", routing_key="reducers-master", body=json.dumps(word_count))
            channel.basic_publish(exchange="exchange", routing_key="reducers-master", body=json.dumps([exit_code, 1]))
            print(f"Reducer{dir_num}: done")
            return
    else:
        if key not in words_dict:
            words_dict[key] = 0
        words_dict[key] += sum(value)

queue = f"shufflers-reduce{dir_num}"
channel.basic_consume(queue, handle_shuffle_respond, auto_ack=True)
channel.start_consuming()
