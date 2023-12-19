import os
import shutil
import glob
import pika
import json
from sys import argv

data_path = argv[1]
mappers_amount = int(argv[2])
reducers_amount = int(argv[3])

wildcard_data_path = f"{data_path}/*.txt"
root_path = ".."
runtime_path = f"{root_path}/runtime"
src_path = f"{root_path}/src"

txt_files = glob.glob(wildcard_data_path)
text_files_amount = len(txt_files)


if not os.path.exists(runtime_path):
    os.makedirs(runtime_path)
else:
    shutil.rmtree(runtime_path)
    os.makedirs(runtime_path)

# Создание папки M1 .. Mn и заполнить их копиями shuffle splitter mapper и текстовыми файлами
k = 0
for i in range(mappers_amount):
    os.makedirs(f"{runtime_path}/M{str(i + 1)}")
    os.system(f"cp {src_path}/shuffle.py {src_path}/splitter.py {src_path}/mapper.py {runtime_path}/M{str(i + 1)}")
    for j in range(text_files_amount // mappers_amount + (i < text_files_amount % mappers_amount)):
        os.system(f"cp {txt_files[k]} {runtime_path}/M{str(i + 1)}")
        k += 1

# Создание папки R1 .. Rk и заполнить их копиями reducer
for i in range(reducers_amount):
    os.makedirs(f"{runtime_path}/R{str(i + 1)}")
    os.system(f"cp {src_path}/reducer.py {runtime_path}/R{str(i + 1)}")

# Запуск splitter
for i in range(mappers_amount):
    os.system(f"python3 {runtime_path}/M{str(i + 1)}/splitter.py {str(i + 1)}")

# Запуск mapper
for i in range(mappers_amount):
    os.system(f"python3 {runtime_path}/M{str(i + 1)}/mapper.py {str(i + 1)}")

# Запуск shuffle
for i in range(mappers_amount):
    os.system(f"python3 {runtime_path}/M{str(i + 1)}/shuffle.py {str(i + 1)} {str(reducers_amount)}")

# Запуск reducer
for i in range(reducers_amount):
    os.system(f"python3 {runtime_path}/R{str(i + 1)}/reducer.py {str(i + 1)} {str(mappers_amount)}")


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='exchange', exchange_type='direct')
channel.queue_declare(queue='reducers-master', auto_delete=True)
channel.queue_bind(exchange='exchange', queue='reducers-master')

exit_code = "-1"
reducers_exited = 0
result = {}

def handle_reduce_response(channel, method, properties, body):
    global reducers_exited
    message = json.loads(body)
    word = message[0]
    count = message[1]
    if word == exit_code:
        reducers_exited += 1
        if reducers_exited == reducers_amount:
            channel.stop_consuming()
            return
    else:
        if word not in result:
            result[word] = 0
        result[word] = count


channel.basic_consume('reducers-master', handle_reduce_response, auto_ack=True)
channel.start_consuming()

for word, amount in sorted(result.items()):
    print(f"{word:>10} - {amount}")
