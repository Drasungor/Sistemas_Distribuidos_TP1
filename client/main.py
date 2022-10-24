import socket
import multiprocessing as mp
import json
import base64
import logging
import errno
from communication_socket import CommunicationSocket
from sigterm_notifier import SigtermNotifier
from files_sender import send_files_data

config_file_path = "config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(config_file)

def send_connection_data(skt: CommunicationSocket, connections_amount: int, files_paths):
    categories = {}
    for country_files in files_paths:
        category_file_path: str = country_files["category"]
        country_prefix = category_file_path.split("/")[2][0:2]
        categories[country_prefix] = get_categories_dict(category_file_path)

    data_dict = { "connections_amount": connections_amount, "files_amount": len(files_paths), "categories": categories }
    skt.send_json(data_dict)

def get_categories_dict(json_path: str):
    categories = {}
    with open(json_path) as category_file_ptr:
        category_dictionary = json.load(category_file_ptr)
        categories_array = category_dictionary["items"]
        for category in categories_array:
            categories[str(category["id"])] = category["snippet"]["title"]
    return categories


def receive_query_response(skt: CommunicationSocket, child_processes):
    sigterm_notifier = SigtermNotifier(child_processes)
    finished = False
    first_query_ptr = open(config["result_files_paths"]["first_query"], "w")
    second_query_folder = config["result_files_paths"]["second_query"]
    third_query_ptr = open(config["result_files_paths"]["third_query"], "w")
    while (not finished) and (not sigterm_notifier.received_sigterm):
        try:
            received_message = skt.read_json()
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


    connection_address = config["accepter_address"]
    connection_port = config["accepter_port"]
    main_process_connection_socket = CommunicationSocket()
    main_process_connection_socket.connect(connection_address, connection_port)

    had_communication_issue = False
    try:
        send_connection_data(main_process_connection_socket, processes_amount, files_paths)
    except Exception as e:
        had_communication_issue = True
        logging.info(f"Cought unexpected exception while talking to server: {str(e)}")

    if not had_communication_issue:
        files_paths_queue = mp.Queue()
        child_processes: "list[mp.Process]" = []
        for _ in range(processes_amount):
            child_processes.append(mp.Process( target = send_files_data, args = [files_paths_queue]))

        for process in child_processes:
            process.start()
        
        for paths in files_paths:
            files_paths_queue.put(paths)
        for _ in range(len(child_processes)):
            files_paths_queue.put(None)

        files_paths_queue.close()
        receive_query_response(main_process_connection_socket, child_processes)

        files_paths_queue.join_thread()
        logging.info("Closed processes queue")
        for process in child_processes:
            process.join()
        logging.info("Joined child processes")
    main_process_connection_socket.close()
    logging.info("Closed main process socket")

if __name__ == "__main__":
    main()