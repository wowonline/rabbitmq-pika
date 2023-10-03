import pika, time
import json


with open("roads-seoul.geojson") as file:
# with open("vienna-streets.geojson") as file:
    data = json.load(file)
    roads = data["features"]

    amountWorkers = 5
    roadsByWorkerId = {}
    # road will be distributed to N'th worker
    # if hash(id) % amountWorkers == N

    for road in roads:
        roadId = road["id"]
        workerId = str(hash(roadId) % amountWorkers)
        if workerId not in roadsByWorkerId:
            roadsByWorkerId[workerId] = []
        roadsByWorkerId[workerId].append(road)
    
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host = "localhost")
    )
    channel = connection.channel()

    # Отправляем в Стартер, сколько worker-процессов нужно запустить
    starterQueueName = "starter_queue"
    channel.queue_declare(queue=starterQueueName, auto_delete=True)
    channel.basic_publish(
        exchange='', routing_key=starterQueueName,
        body=str(amountWorkers)
    )

    time.sleep(1)

    # Отправляем воркерам их дороги
    channel.exchange_declare(exchange="common_exchange", exchange_type="direct")
    for i in range(amountWorkers):
        i = str(i)
        channel.basic_publish(
            exchange="common_exchange", routing_key=f"{i}",
            body=f"{json.dumps(roadsByWorkerId[i])}"
        )
        print('From ETL:      Sent roads to worker number', i)

    # Отправляем менеджеру инфу о воркерах и их дорогах
    etlToManager = "etl_to_manager"
    channel.queue_declare(queue=etlToManager, auto_delete=True)
    
    workerIdRoadsAmount = {}
    for i in range(amountWorkers):
        i = str(i)
        workerIdRoadsAmount[i] = len(roadsByWorkerId[i])

    channel.basic_publish(
        exchange="",
        routing_key="etl_to_manager",
        body=json.dumps(workerIdRoadsAmount)
    )

    connection.close()