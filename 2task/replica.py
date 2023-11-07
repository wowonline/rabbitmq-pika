import pika
import json
import os


number_of_replica = int(os.environ.get('number_of_replica'))
print(f"Replica {number_of_replica}: Starting")
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='replicas', exchange_type='direct')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
x, y, z = 0, 0, 0

channel.queue_bind(queue=queue_name, exchange='replicas', routing_key="replica" + str(number_of_replica))


cached_requests = {}
wanted_request_id = 0
def on_message_callback(ch, method, properties, body):
    global wanted_request_id, x, y, z
    if body.decode() == "stop":
        ch.basic_ack(method.delivery_tag)
        ch.stop_consuming()
        return
    ch.basic_ack(method.delivery_tag)

    for request in json.loads(body):
        request_splitted = request.split()
        req_id = int(request_splitted[0])
        cached_requests[req_id] = " ".join(request_splitted[1:])
        print(f"Replica {number_of_replica}: Got request {req_id}: {cached_requests[req_id]}")
        while wanted_request_id in cached_requests:
            print(f"Replica {number_of_replica}: Updating values by request \
                    {wanted_request_id}: {cached_requests[wanted_request_id]}")
            request_splitted = cached_requests[wanted_request_id].split()
            op = request_splitted[0]
            var = request_splitted[1]
            val_str = request_splitted[2]
            if op == "add":
                val = int(val_str)
                if var == 'x':
                    x += val
                elif var == 'y':
                    y += val
                elif var == 'z': 
                    z += val
            elif op == "mul":
                val = int(val_str)
                if var == 'x':
                    x *= val
                elif var == 'y':
                    y *= val
                elif var == 'z': 
                    z *= val
            del cached_requests[wanted_request_id]

            wanted_request_id += 1
            print(f"Replica {number_of_replica}: Has values: x = {x}, y = {y}, z = {z}")


channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback)
channel.start_consuming()
connection.close()
print(f"Replica {number_of_replica} shut down")
