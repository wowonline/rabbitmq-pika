import pika
import random


set_of_variables = set(["x", "y", "z"])

class Client:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.response = None
        self.correlation_id = None
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_message_callback,
            auto_ack=True
        )


    def on_message_callback(self, ch, method, properties, body):
        if self.correlation_id == properties.correlation_id:
            self.response = body

    def call(self, command):
        self.response = None
        self.correlation_id = str(random.randint(1, 1000))
        self.channel.basic_publish(
            exchange='',
            routing_key='queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.correlation_id,
            ),
            body=str(command)
        )
        self.connection.process_data_events(time_limit=None)
        return self.response


client = Client()
print("Client: Starting")

def is_valid(args):
    if args[0] != "add" and args[0] != "mul":
        return False
    if args[1] not in set_of_variables:
        return False
    if args[2].isnumeric():
        return True
    else:
        return False

while True:
    try:
        command = input()
        args = command.split()
        if command != "stop" and not is_valid(args):
            print("Wrong command")
        else:
            response = client.call(command).decode()
            print(response)
    except (KeyboardInterrupt, EOFError):
        print("\nClient exited")
        break
