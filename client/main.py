import csv
import socket
import signal
import multiprocessing as mp
import queue
import json

config_file_path = "config.json"
config = json.load(open(config_file_path, "r"))

def send_file(files_paths_queue: mp.Queue):
    connection_address = config["accepter_address"]
    connection_port = config["accepter_port"]
    process_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    process_socket.connect((connection_address, connection_port))
    while #cola no lee None


def main():
    files_paths = config["files_names"]

    # Total amount of processes the client is composed of
    processes_amount = min([config["processes_amount"], mp.cpu_count(), len(files_paths)])

if __name__ == "__main__":
    main()