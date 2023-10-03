import pika, os, sys

def IsCorrect(input):
    return input.isnumeric() or input == "exit" or input == "stop" or input == "list"

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host = "localhost")
    )
    channel = connection.channel()



    clientToManager = "client_to_manager"
    channel.queue_declare(queue=clientToManager, auto_delete=True)



    while (True):
        inp = input("Enter a command: ")
        if not IsCorrect(inp):
            print("Incorrect input!")
            continue
        channel.basic_publish(
            exchange="",
            routing_key=clientToManager,
            body=inp
        )
        if inp == "exit" or inp == "stop":
            break

        def Print(ch, method, properties, body):
            print(body.decode())
            channel.stop_consuming()

        channel.queue_declare(queue="manager_to_client", auto_delete=True)
        channel.basic_consume(queue="manager_to_client", on_message_callback=Print)
        channel.start_consuming()

    connection.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)