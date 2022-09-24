import socket
import multiprocessing as mp
import json
# import pika

config_file_path = "config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config["accepter"]

def main():
    accepter_queue = mp.Queue()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('', local_config["bound_port"]))
    server_socket.listen(local_config["listen_backlog"])

    processes_amount = min([config["processes_amount"], mp.cpu_count(), len(files_paths)])



if __name__ == "main":
    main()