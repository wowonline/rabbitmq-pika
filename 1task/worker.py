import pika, json, sys


workerId = sys.argv[-1]


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host = "localhost")
)
channel = connection.channel()

channel.exchange_declare(exchange="common_exchange", exchange_type="direct")
queue = channel.queue_declare(queue="")
queueName = queue.method.queue

channel.queue_bind(
    exchange="common_exchange",
    queue=queueName,
    routing_key=workerId
)

# Получаем дороги, соответствующие workerId
def getRoadsFromEtl(channel, method, properties, body):
    print(f"From Worker {workerId}: Got roads")
    global roads
    roads = json.loads(body.decode())
    channel.stop_consuming()

channel.basic_consume(queue=queueName, on_message_callback=getRoadsFromEtl)
channel.start_consuming()

channel.queue_declare(queue='workers-manager')
channel.queue_bind(exchange='common_exchange', queue='workers-manager')

# Отвечаем на запросы от менеджера
def sendRoadsToManager(channel, method, properties, body):
    channel.basic_publish(
        exchange='common_exchange',
        routing_key='workers-manager',
        body=json.dumps(roads)
    )

channel.basic_consume(queue=queueName, on_message_callback=sendRoadsToManager)
channel.start_consuming()
