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

def send_file_data(skt: socket, file_path: str):
    batch_size = config["batch_size"]
    with open(file_path) as file_ptr:
        csv_reader = csv.reader(file_ptr)
        next(csv_reader) #Discards header
        lines_accumulator = []
        for line in csv_reader:
            lines_accumulator.append(line)
            if len(lines_accumulator) == batch_size:
                dict = { "data": json.dumps(lines_accumulator) }
                lines_accumulator = []
                send_string(skt, dict)
        if len(lines_accumulator) != 0:
            dict = { "data": json.dumps(lines_accumulator) }
            lines_accumulator = []
            send_string(skt, dict)

def send_files_data(files_paths_queue: mp.Queue):
    connection_address = config["accepter_address"]
    connection_port = config["accepter_port"]
    process_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    process_socket.connect((connection_address, connection_port))
    
    read_message = files_paths_queue.get()
    while read_message != None:

        read_message = files_paths_queue.get()



def main():
    files_paths = config["files_names"]

    # Total amount of processes the client is composed of
    processes_amount = min([config["processes_amount"], mp.cpu_count(), len(files_paths)])

if __name__ == "__main__":
    main()