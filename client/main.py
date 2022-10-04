import csv
import socket
import multiprocessing as mp
import json
import base64
import signal

class ClosedSocket(Exception):
	pass

config_file_path = "config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(config_file)

def send_string(skt: socket, data: str):
    encoded_data = data.encode()
    message_length = len(encoded_data)
    skt.sendall(message_length.to_bytes(4, "big"))
    skt.sendall(data.encode())


def __recv_all(skt: socket, bytes_amount: int):
		total_received_bytes = b''
		while (len(total_received_bytes) < bytes_amount):
			received_bytes = skt.recv(bytes_amount - len(total_received_bytes))
			if (len(received_bytes) == 0):
				raise ClosedSocket
			total_received_bytes += received_bytes
		return total_received_bytes

def read_string(skt: socket):
    string_length = int.from_bytes(__recv_all(skt, 4), "big")
    read_string = __recv_all(skt, string_length).decode()
    return read_string

def send_number(skt: socket, number: int):
    skt.sendall(number.to_bytes(4, "big"))

def send_connection_data(skt: socket, connections_amount: int, files_paths):
    categories = {}
    for country_files in files_paths:
        category_file_path: str = country_files["category"]
        country_prefix = category_file_path.split("/")[2][0:2]
        # with open(category_file_path, "r") as current_category_file:
        #     aux = json.load(current_category_file)
        categories[country_prefix] = get_categories_dict(category_file_path)

    data_dict = { "connections_amount": connections_amount, "files_amount": len(files_paths), "categories": categories }
    send_string(skt, json.dumps(data_dict))


def send_cached_data(skt: socket, data, country_prefix: str, file_finished: bool):
    dict = { "data": data, "file_finished": file_finished, "country": country_prefix }
    send_string(skt, json.dumps(dict))

def get_categories_dict(json_path: str):
    categories = {}
    with open(json_path) as category_file_ptr:
        category_dictionary = json.load(category_file_ptr)
        categories_array = category_dictionary["items"]
        for category in categories_array:
            categories[str(category["id"])] = category["snippet"]["title"]
    return categories

def get_next_line(csv_reader):
    should_keep_reading = True
    current_line = None
    while should_keep_reading:
        try:
            current_line = next(csv_reader, None)
            should_keep_reading = False
        except csv.Error:
            print("Reading error")
    return current_line

def send_file_data(skt: socket, files_paths):

    print("Entre a send_file_data")

    batch_size = config["batch_size"]

    # TODO: ENVIAR LOS ARCHIVOS DE CATEGORIAS, EL JOIN TIENE QUE HACERSE EN EL SERVER, probablemente en el duplicator filter, 
    #       TAMBIEN DEBERÍA AGREGARSE EN EL SERVER EL PAIS EN CADA LINEA
    categories = get_categories_dict(files_paths["category"])
    trending_file_path: str = files_paths["trending"]
    country_prefix = trending_file_path.split("/")[2][0:2]
    with open(trending_file_path) as trending_file_ptr:
        csv_reader = csv.reader(trending_file_ptr)
        # csv_reader = csv.reader(trending_file_ptr, delimiter = ",", quotechar='"')
        next(csv_reader) #Discards header
        lines_accumulator = []
        # current_line = next(csv_reader, None)
        current_line = get_next_line(csv_reader)
        while current_line != None:
            if len(current_line) != 16:
                print(f"Expected length 16, got length {len(current_line)} with line {current_line}")
            # category_id = str(current_line[config["category_id_index"]])
            # if category_id in categories:
            #     current_line.append(categories[category_id])
            # else:
            #     current_line.append(None)
            # current_line.append(country_prefix)
            lines_accumulator.append(current_line)
            if len(lines_accumulator) == batch_size:
                send_cached_data(skt, lines_accumulator, country_prefix, False)
                lines_accumulator = []
            # current_line = next(csv_reader, None)
            current_line = get_next_line(csv_reader)
        send_cached_data(skt, lines_accumulator, country_prefix, True)

def send_files_data(files_paths_queue: mp.Queue):
    # print("BORRAR nuevo proceso")
    connection_address = config["accepter_address"]
    connection_port = config["accepter_port"]
    process_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    process_socket.connect((connection_address, connection_port))
    
    read_message = files_paths_queue.get()
    should_keep_iterating = read_message != None
    while should_keep_iterating:
        send_file_data(process_socket, read_message)
        read_message = files_paths_queue.get()
        should_keep_iterating = read_message != None
        if should_keep_iterating:
            send_string(process_socket, json.dumps(True))

    send_string(process_socket, json.dumps(False))
    process_socket.close()

def receive_query_response(skt: socket):
    print("BORRAR entre a receive_query_response")

    finished = False
    first_query_ptr = open(config["result_files_paths"]["first_query"], "w")
    second_query_folder = config["result_files_paths"]["second_query"]
    third_query_ptr = open(config["result_files_paths"]["third_query"], "w")
    while not finished:
        received_message = json.loads(read_string(skt))
        print(f"BORRAR Lei el mensaje {received_message}")
        finished = received_message["finished"]
        if not finished:
            query_type = received_message["type"]
            value = received_message["value"]
            if not finished:
                if query_type == "first_query":
                    first_query_ptr.write(f"{value}\n")
                elif query_type == "second_query":
                    image_bytes = base64.b64decode(value[1])
                    video_id = value[0]
                    aux_thumbnail_file_ptr = open(f"{second_query_folder}/{video_id}", "w")
                    aux_thumbnail_file_ptr.write(image_bytes)
                    aux_thumbnail_file_ptr.close()
                elif query_type == "third_query":
                    third_query_ptr.write(f"{value}\n")
    first_query_ptr.close()
    third_query_ptr.close()


def main():
    files_paths = config["files_names"]

    # Total amount of processes the client is composed of
    processes_amount = min([config["processes_amount"], mp.cpu_count(), len(files_paths)])
    files_paths_queue = mp.Queue()

    child_processes: "list[mp.Process]" = []
    # TODO: ADD FOR LOOP UNTIL RANGE(processes_amount)
    # child_processes.append(mp.Process( target = send_files_data, args = [files_paths_queue]))
    for _ in range(processes_amount):
        child_processes.append(mp.Process( target = send_files_data, args = [files_paths_queue]))


    for process in child_processes:
        process.start()

    connection_address = config["accepter_address"]
    connection_port = config["accepter_port"]
    main_process_connection_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    main_process_connection_socket.connect((connection_address, connection_port))

    # send_number(main_process_connection_socket, len(files_paths))
    # send_connection_data(main_process_connection_socket, len(child_processes), len(files_paths))
    send_connection_data(main_process_connection_socket, len(child_processes), files_paths)
    
    for paths in files_paths:
        files_paths_queue.put(paths)
    for _ in range(len(child_processes)):
        files_paths_queue.put(None)
    files_paths_queue.close()

    # TODO: WAIT FOR QUERY RESPONSE
    receive_query_response(main_process_connection_socket)

    files_paths_queue.join_thread()
    for process in child_processes:
        process.join()

if __name__ == "__main__":
    main()