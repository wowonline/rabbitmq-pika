import json, pika, sys


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host = "localhost")
)
global channel
channel = connection.channel()


channel.exchange_declare(exchange="common_exchange", exchange_type="direct")
channel.queue_declare(queue='workers-manager')
channel.queue_bind(exchange='common_exchange', queue='workers-manager')


def EtlToManagerCallback(channel, method, properties, body):
    global idsToLengths
    idsToLengths = json.loads(body.decode())
    channel.stop_consuming()

# Получить id воркеров и количества их дорог от ETL
etlToManager = "etl_to_manager"
channel.queue_declare(queue=etlToManager, auto_delete=True)
channel.basic_consume(queue=etlToManager, on_message_callback=EtlToManagerCallback)
channel.start_consuming()

# maxRoadId не включительно
maxRoadId = sum([int(idsToLengths[roadsAmount]) for roadsAmount in idsToLengths])

for roadsAmount in idsToLengths:
    print(idsToLengths[roadsAmount])

def PollRequestsFromClient(ch, method, properties, body):
    body = body.decode()
    if body == "list":
        print("Printed list")
        channel.basic_publish(exchange="", routing_key="manager_to_client", body=f"From MANAGER:\n\tThere's your list of roads: {str([i for i in range(maxRoadId)])}")
    elif body == "stop" or body == "exit":
        print("Stopping!")

    elif int(body) >= maxRoadId:
        print("Number out of bounds")
        channel.basic_publish(exchange="", routing_key="manager_to_client", body="From MANAGER:\n\tNo such road!")
    else:
        # Получили id дороги, которую надо вернуть
        global roadId
        roadId = int(body)
        # if (roadId >= maxRoadId):
        #     print("No such road!")
        #     # channel.basic_publish(exchange="", routing_key="manager_to_client", body="No such road!")
        #     return

        # Вычисляем номер воркера
        foundWorkerId = 0
        for workerId in range(len(idsToLengths)):
            workerId = str(workerId)
            if roadId - idsToLengths[workerId] < 0:
                foundWorkerId = workerId
                break
            else:
                roadId -= idsToLengths[workerId]

        print(f"From Manager:  asked Worker {foundWorkerId} for roads")
        channel.basic_publish(
            exchange="common_exchange",
            routing_key=f"{foundWorkerId}", body="ping"
        )

        channel.stop_consuming()

channel.queue_declare(queue="manager_to_client", auto_delete=True)

while(True):
    clientToManager = "client_to_manager"
    channel.queue_declare(queue=clientToManager, auto_delete=True)
    channel.basic_consume(queue=clientToManager, on_message_callback=PollRequestsFromClient)
    channel.start_consuming()

    def GetRoads(ch, method, properties, body):
        global roads
        roads = json.loads(body.decode())
        channel.stop_consuming()

    channel.basic_consume(queue="workers-manager", on_message_callback=GetRoads) 
    channel.start_consuming()
    channel.basic_publish(exchange="", routing_key="manager_to_client", body=f"From MANAGER:\n\tHere's your road: {roads[roadId]['geometry']['coordinates']}")
