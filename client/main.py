import csv
import socket
import multiprocessing as mp
import json
import base64
import signal
import logging
import errno
from communication_socket import CommunicationSocket, ClosedSocket

config_file_path = "config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(config_file)

# class SigtermNotifier:
#     def __init__(self, processes = None):
#         self.received_sigterm = False
#         self.processes = processes
#         signal.signal(signal.SIGTERM, self.__handle_sigterm)

#     def __handle_sigterm(self, *args):
#         self.received_sigterm = True
#         if self.processes != None:
#             for process in self.processes:
#                 process.terminate()

# sigterm_notifier = SigtermNotifier()

# class SigtermNotifier:
#     def __init__(self, pool = None):
#         self.received_sigterm = False
#         self.pool = pool
#         signal.signal(signal.SIGTERM, self.__handle_sigterm)

#     def __handle_sigterm(self, *args):
#         self.received_sigterm = True
#         if self.pool != None:
#             # logging.debug("BORRAR entre al sigterm en un hijo con if en true")
#             # self.pool.close()
#             self.pool.terminate()
#         # else:
#         #     logging.debug("BORRAR entre al sigterm en un hijo")

class SigtermNotifier:
    def __init__(self, pool = None, skt = None):
        self.received_sigterm = False
        self.pool = pool
        self.skt = skt
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

    def __handle_sigterm(self, *args):
        self.received_sigterm = True
        if self.pool != None:
            # logging.debug("BORRAR entre al sigterm en un hijo con if en true")
            # self.pool.close()
            self.pool.terminate()
        if self.skt != None:
            self.skt.close()
            logging.debug("ESTOY EN SIGTERM EL MAIN PROCESS LA PUTA MADRE")
        # else:
        #     logging.debug("BORRAR entre al sigterm en un hijo")



def send_connection_data(skt: CommunicationSocket, connections_amount: int, files_paths):
    categories = {}
    for country_files in files_paths:
        category_file_path: str = country_files["category"]
        country_prefix = category_file_path.split("/")[2][0:2]
        categories[country_prefix] = get_categories_dict(category_file_path)

    data_dict = { "connections_amount": connections_amount, "files_amount": len(files_paths), "categories": categories }
    skt.send_json(data_dict)

def send_cached_data(skt: CommunicationSocket, data, country_prefix: str, file_finished: bool):
    dict = { "data": data, "file_finished": file_finished, "country": country_prefix, "should_continue_communication": True }
    skt.send_json(dict)

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
            logging.info("Reading error")
    return current_line

def send_file_data(skt: socket, files_paths, sigterm_notifier: SigtermNotifier):
    batch_size = config["batch_size"]
    trending_file_path: str = files_paths["trending"]
    country_prefix = trending_file_path.split("/")[2][0:2]
    with open(trending_file_path) as trending_file_ptr:
        csv_reader = csv.reader(trending_file_ptr)
        next(csv_reader) #Discards header
        lines_accumulator = []
        current_line = get_next_line(csv_reader)
        while (current_line != None) and (not sigterm_notifier.received_sigterm):
            lines_accumulator.append(current_line)
            if len(lines_accumulator) == batch_size:
                send_cached_data(skt, lines_accumulator, country_prefix, False)
                lines_accumulator = []
            current_line = get_next_line(csv_reader)
        send_cached_data(skt, lines_accumulator, country_prefix, True)

# def send_files_data(files_paths_queue: mp.Queue):
#     sigterm_notifier = SigtermNotifier()
#     connection_address = config["accepter_address"]
#     connection_port = config["accepter_port"]

#     read_message = files_paths_queue.get()
#     should_keep_iterating = read_message != None
#     while should_keep_iterating:
#         process_socket = CommunicationSocket()
#         process_socket.connect(connection_address, connection_port)
#         if not sigterm_notifier.received_sigterm:
#             send_file_data(process_socket, read_message, sigterm_notifier)
#         process_socket.send_json({ "should_continue_communication": False })
#         process_socket.close()
#         logging.info("Closed process socket")
#         read_message = files_paths_queue.get()
#         should_keep_iterating = read_message != None

def send_files_data(paths):
    sigterm_notifier = SigtermNotifier()
    connection_address = config["accepter_address"]
    connection_port = config["accepter_port"]
    process_socket = CommunicationSocket()
    process_socket.connect(connection_address, connection_port)
    # if not sigterm_notifier.received_sigterm:
    #     send_file_data(process_socket, files_paths, sigterm_notifier)
    send_file_data(process_socket, paths, sigterm_notifier)
    process_socket.send_json({ "should_continue_communication": False })
    process_socket.close()
    logging.info("Closed process socket")


def receive_query_response(skt: CommunicationSocket, pool):
# def receive_query_response(skt: CommunicationSocket, child_processes):
# def receive_query_response(skt: CommunicationSocket, sigterm_notifier: SigtermNotifier):
    # sigterm_notifier = SigtermNotifier(child_processes)
    # sigterm_notifier = SigtermNotifier(pool)
    sigterm_notifier = SigtermNotifier(pool, skt)
    finished = False
    first_query_ptr = open(config["result_files_paths"]["first_query"], "w")
    second_query_folder = config["result_files_paths"]["second_query"]
    third_query_ptr = open(config["result_files_paths"]["third_query"], "w")
    while (not finished) and (not sigterm_notifier.received_sigterm):
        try:
            logging.debug("VOY A LEER DEL SOCKET")
            received_message = skt.read_json()
            logging.debug("LEI DEL SOCKET")
            finished = received_message["finished"]
        except socket.error as e:
            if e.errno == errno.EPIPE:
                logging.info(f"Server has already closed the connection")
            else:
                logging.info(f"Caught unexpected exception while receiving message from server: {str(e)}")
            finished = True
        if not finished:
            query_type = received_message["type"]
            value = received_message["value"]
            if not finished:
                if query_type == "first_query":
                    first_query_ptr.write(f"{value}\n")
                elif query_type == "second_query":
                    image_bytes = base64.b64decode(value[1])
                    video_id = value[0]
                    aux_thumbnail_file_ptr = open(f"{second_query_folder}/{video_id}", "wb")
                    aux_thumbnail_file_ptr.write(image_bytes)
                    aux_thumbnail_file_ptr.close()
                elif query_type == "third_query":
                    third_query_ptr.write(f"{value}\n")
    first_query_ptr.close()
    third_query_ptr.close()
    

def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level="DEBUG",
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    files_paths = config["files_names"]

    # Total amount of processes the client is composed of
    processes_amount = min([config["processes_amount"], mp.cpu_count(), len(files_paths)])
    # files_paths_queue = mp.Queue()


    connection_address = config["accepter_address"]
    connection_port = config["accepter_port"]
    main_process_connection_socket = CommunicationSocket()
    main_process_connection_socket.connect(connection_address, connection_port)


    send_connection_data(main_process_connection_socket, processes_amount, files_paths)

    # child_processes: "list[mp.Process]" = []
    # for _ in range(processes_amount):
    #     child_processes.append(mp.Process( target = send_files_data, args = [files_paths_queue]))

    # for process in child_processes:
    #     process.start()
    
    # for paths in files_paths:
    #     files_paths_queue.put(paths)
    # for _ in range(len(child_processes)):
    #     files_paths_queue.put(None)

    process_pool = mp.Pool()
    process_pool.map_async(send_files_data, files_paths)


    # files_paths_queue.close()
    # receive_query_response(main_process_connection_socket, child_processes)
    
    receive_query_response(main_process_connection_socket, process_pool)
    # receive_query_response(main_process_connection_socket, sigterm_notifier)
    main_process_connection_socket.close()
    logging.info("Closed main process socket")

    logging.debug("VOY A JOINEAR")

    process_pool.close()
    process_pool.join()

    logging.debug("JOINEE")

    # files_paths_queue.join_thread()
    # logging.info("Closed processes queue")
    # for process in child_processes:
    #     process.join()
    logging.info("Joined child processes")

if __name__ == "__main__":
    main()