import socket
import multiprocessing as mp
import json
from MOM.MOM import MOM

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
    def __init__(self, skt: socket, middleware: MOM):
        self.socket = skt
        self.middleware = middleware
        self.received_eofs = 0

        self.previous_stage_size = 0
        for previous_stage in local_config["receives_from"]:
            self.previous_stage_size += config[previous_stage]["computers_amount"]


    def process_received_message(self, ch, method, properties, body):
        response = json.loads(body)
        if response != None:
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                send_json(self.socket, { "finished": True })
        else:
            sender = response["type"]
            if sender == "duplication_filter":
                received_tuple = response["tuple"]
                send_json(self.socket, { "first_query": received_tuple, "finished": False })
            elif sender == "thumbnails_downloader":
                max_day = response["max_day"]
                send_json(self.socket, { "second_query": max_day, "finished": False })
            elif sender == "max_views_day":
                image_data = response["img_data"]
                send_json(self.socket, { "third_query": image_data, "finished": False })
            else:
                raise ValueError(f"Unexpected sender: {sender}")

def read_json(skt: socket):
    return json.loads(__read_string(skt))

def send_string(skt: socket, data: str):
    encoded_data = data.encode()
    message_length = len(encoded_data)
    skt.sendall(message_length.to_bytes(4, "big"))
    skt.sendall(data.encode())

def send_json(skt: socket, data):
    send_string(skt, json.dumps(data))

def handle_connection(connections_queue: mp.Queue, middleware: MOM):
    # middleware = MOM(cluster_type, None)
    read_socket = connections_queue.get()
    while read_socket != None:
        should_keep_iterating = True

        # BORRAR
        read_lines = 0

        while should_keep_iterating:
            # dict = { "data": json.dumps(data), "file_finished": file_finished }
            read_data = read_json(read_socket)
        
            # BORRAR
            read_lines += len(read_data["data"])

            should_keep_iterating = not read_data["file_finished"]
            if len(read_data["data"]) != 0:
                for line in read_data["data"]:
                    middleware.send_line(line)
                    pass
                    # print(f"Category: {line[0]}")
        middleware.send_general(None)
        # BORRAR
        print(f"Read lines: {read_lines}")

        read_socket = connections_queue.get()

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
    accepter_queue = mp.Queue()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('', local_config["bound_port"]))
    server_socket.listen(local_config["listen_backlog"])

    first_connection, _ = server_socket.accept()

    connections_data = read_json(first_connection)

    middleware = MOM(cluster_type, None)

    incoming_connections = connections_data["connections_amount"]
    incoming_files_amount = connections_data["files_amount"]
    processes_amount = min([local_config["processes_amount"], mp.cpu_count(), incoming_connections])

    middleware.send_general(processes_amount) # So that the other clusters know for how many Nones they have to listen to

    child_processes: "list[mp.Queue]" = []
    for _ in range(processes_amount):
        new_process = mp.Process( target = handle_connection, args = [accepter_queue, middleware])
        child_processes.append(new_process)
        new_process.start()
    for _ in range(incoming_connections):
        accepted_socket, _ = server_socket.accept()
        accepter_queue.put(accepted_socket)
    
    for _ in range(len(child_processes)):
        accepter_queue.put(None)

    # start receiving MOM messages

    accepter_queue.close()
    accepter_queue.join_thread()
    for i in range(len(child_processes)):
        child_processes[i].join()

if __name__ == "__main__":
    main()