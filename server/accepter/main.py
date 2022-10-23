import multiprocessing as mp
import json
from MOM import MOM
import signal
from accepter_socket import AccepterSocket
import logging
from accepter import Accepter

cluster_type = "accepter"

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config[cluster_type]


class SigtermNotifier:
    def __init__(self):
        self.received_sigterm = False
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

    def __handle_sigterm(self, *args):
        self.received_sigterm = True

def handle_connection(connections_queue: mp.Queue, categories):
    middleware = MOM(f"{cluster_type}_sender", None)
    read_socket = connections_queue.get()
    sigterm_notifier = SigtermNotifier()

    while read_socket != None :
        should_keep_iterating = True

        while should_keep_iterating and (not sigterm_notifier.received_sigterm):
            read_data = read_socket.read_json()
            if read_data["should_continue_communication"]:
                batch_country_prefix = read_data["country"]
                current_country_categories = categories[batch_country_prefix]
                if len(read_data["data"]) != 0:
                    for line in read_data["data"]:
                        indexes = local_config["indexes"]
                        category_index = indexes["category"]
                        category_id = str(line[category_index])
                        if category_id in current_country_categories:
                            line[category_index] = current_country_categories[category_id]
                        else:
                            line[category_index] = None
                        line.append(batch_country_prefix)
                        middleware.send_line(line)
            else:
                should_keep_iterating = False
        read_socket.close()
        read_socket = connections_queue.get()
    middleware.send_general(None)
    middleware.close()
    print("Closed subprocess MOM")

def main():
    # logging.basicConfig(
    #     format='%(asctime)s %(levelname)-8s %(message)s',
    #     level="DEBUG",
    #     datefmt='%Y-%m-%d %H:%M:%S',
    # )
    accepter_queue = mp.Queue()
    server_socket = AccepterSocket(local_config["bound_port"], local_config["listen_backlog"])
    first_connection = server_socket.accept()
    connections_data = first_connection.read_json()
    categories = connections_data["categories"]
    incoming_connections = connections_data["connections_amount"]
    incoming_files_amount = connections_data["files_amount"]
    processes_amount = local_config["processes_amount"]

    child_processes: "list[mp.Queue]" = []
    for _ in range(processes_amount):
        new_process = mp.Process( target = handle_connection, args = [accepter_queue, categories])
        child_processes.append(new_process)

    accepter_object = Accepter(first_connection, child_processes)
    accepter_object.send_general(incoming_files_amount) # Send the amount of countries

    for process in child_processes:
        process.start()

    # for _ in range(incoming_connections):
    for _ in range(incoming_files_amount):
        accepted_socket = server_socket.accept()
        accepter_queue.put(accepted_socket)
    
    for _ in range(len(child_processes)):
        accepter_queue.put(None)

    accepter_object.start_received_messages_processing()

    first_connection.close()
    print("Closed first client connection socket")
    server_socket.close()
    print("Closed accepter socket")

    accepter_queue.close()
    accepter_queue.join_thread()
    print("Closed processes queue")
    for i in range(len(child_processes)):
        child_processes[i].join()
    print("Joined child processes")

if __name__ == "__main__":
    main()