import pika
import json
import os
import random

K = int(os.environ.get('K', 1))
N = int(os.environ.get('N', 1))

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='master_node')
replica_channel = connection.channel()
channel.exchange_declare(exchange='replicas', exchange_type='direct')

group_of_requests = []
x, y, z = 0, 0, 0

def on_request_from_master(ch, method, properties, body):
    global x, y, z, req_id
    request = body.decode() 
    if request == "stop":
        ch.basic_ack(method.delivery_tag)
        ch.stop_consuming()
        return
    
    print(f"Node: Got request {request}")
    group_of_requests.append(request)
    ch.basic_ack(method.delivery_tag)

    if len(group_of_requests) == N:
        for i in range(K):
            print(f"Node: Send request {request} to replica {i}")
            random.shuffle(group_of_requests)
            replica_channel.basic_publish(
                "replicas",
                routing_key="replica" + str(i),
                body=json.dumps(group_of_requests)
            )
        group_of_requests.clear()

channel.basic_consume(queue='master_node', on_message_callback=on_request_from_master)
channel.start_consuming()

for i in range(K):
    replica_channel.basic_publish("replicas", routing_key="replica" + str(i), body="stop")

connection.close()

print("Node: shut down")