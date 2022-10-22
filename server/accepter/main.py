import socket
import multiprocessing as mp
import json
from MOM import MOM
import signal
import errno
import logging

cluster_type = "accepter"

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config[cluster_type]

class ClosedSocket(Exception):
	pass

class Accepter():
    def __init__(self, skt: socket, child_processes):
        self.socket = skt
        self.middleware: MOM = MOM(cluster_type, self.process_received_message)
        self.received_eofs = 0
        self.child_processes = child_processes
        self.has_to_close = False
        self.previous_stage_size = self.middleware.get_previous_stage_size()

        # signal.signal(signal.SIGTERM, self.__handle_signal)
        signal.signal(signal.SIGTERM, self.send_close_signal)

    def send_general(self, message):
        self.middleware.send_general(message)

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

    def process_received_message(self, ch, method, properties, body):
        response = json.loads(body)

        try:
            if response == None:
                self.received_eofs += 1
                if self.received_eofs == self.previous_stage_size:
                    send_json(self.socket, { "finished": True })
                    self.has_to_close = True
            else:
                sender = response["type"]
                if sender == "duplication_filter":
                    received_tuple = response["tuple"]
                    send_json(self.socket, { "type": "first_query", "value": received_tuple, "finished": False })
                elif sender == "thumbnails_downloader":
                    image_data = response["img_data"]
                    send_json(self.socket, { "type": "second_query", "value": image_data, "finished": False })
                elif sender == "max_views_day":
                    max_day = response["max_day"]
                    send_json(self.socket, { "type": "third_query", "value": max_day, "finished": False })
                else:
                    raise ValueError(f"Unexpected sender: {sender}")
        except socket.error as e:
            if e.errno == errno.EPIPE:
                print(f"Client has already closed the connection")
            else:
                print(f"Caught unexpected exception while receiving message from server: {str(e)}")
            # finished = True
            self.send_close_signal()


        if self.has_to_close:
            self.middleware.close()
            print("Closed MOM")

    # def __handle_signal(self, *args): # To prevent double closing 
    #     self.has_to_close = True
    #     for process in self.child_processes:
    #         process.terminate()
    def send_close_signal(self, *args): # To prevent double closing 
        if not self.has_to_close:
            for process in self.child_processes:
                process.terminate()
        self.has_to_close = True

def read_json(skt: socket):
    return json.loads(__read_string(skt))

def send_string(skt: socket, data: str):
    encoded_data = data.encode()
    message_length = len(encoded_data)
    skt.sendall(message_length.to_bytes(4, "big"))
    skt.sendall(data.encode())

def send_json(skt: socket, data):
    send_string(skt, json.dumps(data))

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
            read_data = read_json(read_socket)
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

def __recv_all(skt: socket, bytes_amount: int):
		total_received_bytes = b''
		while (len(total_received_bytes) < bytes_amount):
			received_bytes = skt.recv(bytes_amount - len(total_received_bytes))
			if (len(received_bytes) == 0):
				raise ClosedSocket
			total_received_bytes += received_bytes
		return total_received_bytes

def __read_string(skt: socket):
    string_length = int.from_bytes(__recv_all(skt, 4), "big")
    read_string = __recv_all(skt, string_length).decode()
    return read_string


def main():
    # logging.basicConfig(
    #     format='%(asctime)s %(levelname)-8s %(message)s',
    #     level="DEBUG",
    #     datefmt='%Y-%m-%d %H:%M:%S',
    # )
    accepter_queue = mp.Queue()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('', local_config["bound_port"]))
    server_socket.listen(local_config["listen_backlog"])

    first_connection, _ = server_socket.accept()

    connections_data = read_json(first_connection)

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

    for _ in range(incoming_connections):
        accepted_socket, _ = server_socket.accept()
        accepter_queue.put(accepted_socket)
    
    for _ in range(len(child_processes)):
        accepter_queue.put(None)

    accepter_object.start_received_messages_processing()

    first_connection.close()
    print("Closed accepter socket")

    accepter_queue.close()
    accepter_queue.join_thread()
    print("Closed processes queue")
    for i in range(len(child_processes)):
        child_processes[i].join()
    print("Joined child processes")

if __name__ == "__main__":
    main()