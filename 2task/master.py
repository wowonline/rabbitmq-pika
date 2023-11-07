import pika


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
data_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='queue')
data_channel = data_connection.channel()
data_channel.queue_declare(queue='master_node')

x, y, z = 0, 0, 0
request_id = 0


def on_message_callback(ch, method, properties, body):
    global x, y, z, request_id
    request = body.decode() 
    print("Master: Got request:", request)
    if request.startswith("stop"):
        ch.basic_ack(method.delivery_tag)
        ch.stop_consuming()
        return
    
    splitted_request = request.split()
    op = splitted_request[0]
    var = splitted_request[1]
    val_str = splitted_request[2]
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
    resp = f"Current values: x = {x}, y = {y}, z = {z}"

    data_channel.basic_publish(
        exchange="",
        routing_key="master_node",
        body=str(request_id) + ' ' + request
    )

    request_id += 1
    ch.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id
        ),
        body=str(resp)
    )
    ch.basic_ack(method.delivery_tag)

channel.basic_consume(queue='queue', on_message_callback=on_message_callback)
channel.start_consuming()

data_channel.basic_publish(
    exchange="",
    routing_key="master_node",
    body = "stop"
)

connection.close()

print("Master: shut down")
