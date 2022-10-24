import csv
import socket
import multiprocessing as mp
import logging
import json
from sigterm_notifier import SigtermNotifier
from communication_socket import CommunicationSocket

config_file_path = "config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(config_file)

def send_cached_data(skt: CommunicationSocket, data, country_prefix: str, file_finished: bool):
    dict = { "data": data, "file_finished": file_finished, "country": country_prefix, "should_continue_communication": True }
    skt.send_json(dict)


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

def send_files_data(files_paths_queue: mp.Queue):
    sigterm_notifier = SigtermNotifier()
    connection_address = config["accepter_address"]
    connection_port = config["accepter_port"]

    read_message = files_paths_queue.get()
    should_keep_iterating = read_message != None
    process_socket = None
    encountered_error = False
    while should_keep_iterating:
        try:
            process_socket = CommunicationSocket()
            process_socket.connect(connection_address, connection_port)
            if (not sigterm_notifier.received_sigterm) and (not encountered_error):
                send_file_data(process_socket, read_message, sigterm_notifier)
        except Exception as e:
            encountered_error = True
            logging.info(f"Unexpected error in file communication: {str(e)}")
        process_socket.send_json({ "should_continue_communication": False })
        process_socket.close()
        logging.info("Closed process socket")
        read_message = files_paths_queue.get()
        should_keep_iterating = read_message != None
