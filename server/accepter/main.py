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

class ClosedSocket(Exception):
	pass

def read_json(skt: socket):
    json.loads(__read_string(skt))

def handle_connection(connections_queue: mp.Queue):
    read_socket = connections_queue.get()
    while read_socket != None:
        should_keep_iterating = True
        while should_keep_iterating:
            # dict = { "data": json.dumps(data), "file_finished": file_finished }
            read_data = read_json(read_socket)
            should_keep_iterating = not read_data["file_finished"]
            if len(read_data["data"]) != 0:
                for line in read_data["data"]:
                    print(f"Category: {line[-1]}")

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
    return __recv_all(string_length).decode()


def main():
    accepter_queue = mp.Queue()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('', local_config["bound_port"]))
    server_socket.listen(local_config["listen_backlog"])

    connections_data = read_json(server_socket)
    incoming_connections = connections_data["connections_amount"]
    processes_amount = min([config["processes_amount"], mp.cpu_count(), incoming_connections])
    child_processes: "list[mp.Queue]" = []
    for _ in range(processes_amount):
        child_processes.append(mp.Process( target = handle_connection, args = [accepter_queue]))
    for _ in range(incoming_connections):
        accepted_socket, address = server_socket.accept()
        accepter_queue.put(accepted_socket)
    
    for _ in range(len(child_processes)):
        accepter_queue.put(None)
    accepter_queue.close()
    accepter_queue.join_thread()
    for i in range(len(child_processes)):
        child_processes[i].join()

if __name__ == "main":
    main()