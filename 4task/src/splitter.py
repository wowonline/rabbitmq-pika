import pika
import glob
from sys import argv
import json

exit_code = "-1"
dir_num = argv[1]


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="exchange", exchange_type="direct")
queue = f"splitter_mapper{dir_num}"
channel.queue_declare(queue=queue, auto_delete=True)
channel.queue_bind(exchange="exchange", queue=queue)


root_path = ".."
wildcard_data_path = f"{root_path}/runtime/M{dir_num}/*.txt"

txt_files = glob.glob(wildcard_data_path)

strings = [] 
for filename in txt_files:
    f = open(filename, "r")
    file_strings = f.read().splitlines()
    for string in file_strings:
        channel.basic_publish(exchange="exchange", routing_key=queue, body=json.dumps(string))

channel.basic_publish(exchange="exchange", routing_key=queue, body=json.dumps(exit_code))
print(f"Splitter{dir_num}: done")
