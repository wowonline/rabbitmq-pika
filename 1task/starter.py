import subprocess, pika, sys, os


def main():
    processes = []

    processes.append(subprocess.Popen(
        "python3 manager.py".split(), stdin=None, stdout=None,
        stderr=None, close_fds=True
    ))

    processes.append(subprocess.Popen(
        "python3 etl.py".split(), stdin=None, stdout=None,
        stderr=None, close_fds=True
    ))

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host = 'localhost')
    )
    channel = connection.channel()

    starterQueueName = 'starter_queue'
    channel.queue_declare(queue=starterQueueName, auto_delete=True)

    def GetAmountWorkers(ch, method, properties, body):
        global amountWorkers
        amountWorkers = int(body.decode())
        channel.stop_consuming()
    
    channel.basic_consume(queue=starterQueueName, on_message_callback=GetAmountWorkers, auto_ack=True)
    channel.start_consuming()

    for i in range(amountWorkers):
        processes.append(subprocess.Popen(
            f"python3 worker.py {i}".split(), stdin=None, stdout=None,
            stderr=None, close_fds=True
        ))

    [p.wait() for p in processes]
    connection.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
