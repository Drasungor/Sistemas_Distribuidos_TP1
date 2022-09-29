import csv
import socket
import multiprocessing as mp
import json
import signal


config_file_path = "config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))

def send_string(skt: socket, data: str):
    encoded_data = data.encode()
    message_length = len(encoded_data)
    skt.sendall(message_length.to_bytes(4, "big"))
    skt.sendall(data.encode())

def send_number(skt: socket, number: int):
    skt.sendall(number.to_bytes(4, "big"))

def send_connection_data(skt: socket, connections_amount: int, files_amount: int):
    data_dict = { "connections_amount": connections_amount, "files_amount": files_amount }
    send_string(skt, json.dumps(data_dict))


def send_cached_data(skt: socket, data, file_finished: bool):
    dict = { "data": data, "file_finished": file_finished }
    send_string(skt, json.dumps(dict))

def get_categories_dict(json_path: str):
    categories = {}
    with open(json_path) as category_file_ptr:
        category_dictionary = json.load(category_file_ptr)
        categories_array = category_dictionary["items"]
        for category in categories_array:
            categories[str(category["id"])] = category["snippet"]["title"]
    return categories

def send_file_data(skt: socket, files_paths):
    batch_size = config["batch_size"]


    # TODO: ENVIAR LOS ARCHIVOS DE CATEGORIAS, EL JOIN TIENE QUE HACERSE EN EL SERVER, probablemente en el duplicator filter, 
    #       TAMBIEN DEBERÍA AGREGARSE EN EL SERVER EL PAIS EN CADA LINEA
    categories = get_categories_dict(files_paths["category"])
    trending_file_path: str = files_paths["trending"]
    country_prefix = trending_file_path.split(".")[2][0:2]
    with open(trending_file_path) as trending_file_ptr:
        csv_reader = csv.reader(trending_file_ptr)
        next(csv_reader) #Discards header
        lines_accumulator = []
        for line in csv_reader:
            category_id = str(line[config["category_id_index"]])
            if category_id in categories:
                line.append(categories[category_id])
            else:
                line.append(None)
            line.append(country_prefix)
            lines_accumulator.append(line)
            if len(lines_accumulator) == batch_size:
                send_cached_data(skt, lines_accumulator, False)
                lines_accumulator = []
        send_cached_data(skt, lines_accumulator, True)

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

    connection_address = config["accepter_address"]
    connection_port = config["accepter_port"]
    main_process_connection_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    main_process_connection_socket.connect((connection_address, connection_port))

    # send_number(main_process_connection_socket, len(files_paths))
    send_connection_data(main_process_connection_socket, len(child_processes), len(files_paths))
    
    for paths in files_paths:
        files_paths_queue.put(paths)
    for _ in range(len(child_processes)):
        files_paths_queue.put(None)
    files_paths_queue.close()

    # TODO: WAIT FOR QUERY RESPONSE

    files_paths_queue.join_thread()
    for process in child_processes:
        process.join()

if __name__ == "__main__":
    main()