import csv
import socket
import signal
import multiprocessing as mp
import queue
import json

config_file_path = "config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))

def send_string(skt: socket, data: str):
    encoded_data = data.encode()
    message_length = len(encoded_data)
    skt.sendall(message_length.to_bytes(4, "big"))
    skt.sendall(data)

def send_cached_data(skt: socket, data):
    dict = { "data": json.dumps(data) }
    send_string(skt, dict)

def get_categories_dict(json_path: str):
    categories = {}
    with open(json_path) as category_file_ptr:
        category_dictionary = json.load(category_file_ptr)
        categories_array = category_dictionary["items"]
        for category in categories_array:
            categories[category["id"]] = category["snippet"]["title"]
    return categories

def send_file_data(skt: socket, files_paths: str):
    batch_size = config["batch_size"]
    categories = get_categories_dict(files_paths["category"])
    with open(files_paths["trending"]) as trending_file_ptr:
        csv_reader = csv.reader(trending_file_ptr)
        next(csv_reader) #Discards header
        lines_accumulator = []
        for line in csv_reader:
            category_id = str(line[5])
            line.append(categories[category_id])
            lines_accumulator.append(line)
            if len(lines_accumulator) == batch_size:
                send_cached_data(skt, lines_accumulator)
                lines_accumulator = []
        if len(lines_accumulator) != 0:
            send_cached_data(skt, lines_accumulator)

def send_files_data(files_paths_queue: mp.Queue):
    connection_address = config["accepter_address"]
    connection_port = config["accepter_port"]
    process_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    process_socket.connect((connection_address, connection_port))
    
    read_message = files_paths_queue.get()
    while read_message != None:
        send_file_data(process_socket, read_message)
        read_message = files_paths_queue.get()
    process_socket.close()

def main():
    files_paths = config["files_names"]

    # Total amount of processes the client is composed of
    processes_amount = min([config["processes_amount"], mp.cpu_count(), len(files_paths)])
    files_paths_queue = mp.Queue()

    child_processes: "list[mp.Process]" = []
    # TODO: ADD FOR LOOP UNTIL RANGE(processes_amount)
    child_processes.append(mp.Process( target = send_files_data, args = [files_paths_queue]))

    for process in child_processes:
        process.start()
    for paths in files_paths:
        files_paths_queue.put(paths)
    for _ in range(len(child_processes)):
        files_paths_queue.put(None)

    # TODO: WAIT FOR QUERY

    for process in child_processes:
        process.join()

if __name__ == "__main__":
    main()