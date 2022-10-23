import socket
import multiprocessing as mp
import json
from MOM import MOM
import signal
import errno
from communication_socket import CommunicationSocket, ClosedSocket
from accepter_socket import AccepterSocket

cluster_type = "accepter"

class Accepter():
    # def __init__(self, skt: CommunicationSocket, child_processes, accepter_skt: AccepterSocket):
    #     self.socket = skt
    #     self.middleware: MOM = MOM(cluster_type, self.process_received_message)
    #     self.received_eofs = 0
    #     self.child_processes = child_processes
    #     self.accepter_skt = accepter_skt
    #     self.has_to_close = False
    #     self.previous_stage_size = self.middleware.get_previous_stage_size()

    #     signal.signal(signal.SIGTERM, self.send_close_signal)

    # def __init__(self, skt: CommunicationSocket, child_processes):
    #     self.socket = skt
    #     self.middleware: MOM = MOM(cluster_type, self.process_received_message)
    #     self.received_eofs = 0
    #     self.child_processes = child_processes
    #     self.has_to_close = False
    #     self.previous_stage_size = self.middleware.get_previous_stage_size()

    #     signal.signal(signal.SIGTERM, self.send_close_signal)

    # def __init__(self, skt: CommunicationSocket):
    #     self.socket = skt
    #     self.middleware: MOM = MOM(cluster_type, self.process_received_message)
    #     self.received_eofs = 0
    #     self.has_to_close = False
    #     self.previous_stage_size = self.middleware.get_previous_stage_size()

    #     signal.signal(signal.SIGTERM, self.send_close_signal)

    def __init__(self, skt: CommunicationSocket, accepter_process):
        self.socket = skt
        self.middleware: MOM = MOM(cluster_type, self.process_received_message)
        self.received_eofs = 0
        self.accepter_process = accepter_process
        self.has_to_close = False
        self.previous_stage_size = self.middleware.get_previous_stage_size()

        signal.signal(signal.SIGTERM, self.send_close_signal)
        print("SETUPEE SIGTERM HANDLER")

    def send_general(self, message):
        self.middleware.send_general(message)

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

    def process_received_message(self, ch, method, properties, body):
        response = json.loads(body)

        try:
            if response == None:
                print("RECIBI UN NONE")
                self.received_eofs += 1
                if self.received_eofs == self.previous_stage_size:
                    print("VOY A ENVIAR FINISHED")
                    self.socket.send_json({ "finished": True })
                    self.has_to_close = True
            else:
                sender = response["type"]
                if sender == "duplication_filter":
                    received_tuple = response["tuple"]
                    self.socket.send_json({ "type": "first_query", "value": received_tuple, "finished": False })
                elif sender == "thumbnails_downloader":
                    image_data = response["img_data"]
                    self.socket.send_json({ "type": "second_query", "value": image_data, "finished": False })
                elif sender == "max_views_day":
                    max_day = response["max_day"]
                    self.socket.send_json({ "type": "third_query", "value": max_day, "finished": False })
                else:
                    raise ValueError(f"Unexpected sender: {sender}")
        except socket.error as e:
            if e.errno == errno.EPIPE:
                print(f"Client has already closed the connection")
            else:
                print(f"Caught unexpected exception while receiving message from server: {str(e)}")
            self.send_close_signal()


        if self.has_to_close:
            self.middleware.close()
            print("Closed MOM")

    # def send_close_signal(self, *args): # To prevent double closing 
    #     if not self.has_to_close:
    #         for process in self.child_processes:
    #             process.terminate()
    #     self.has_to_close = True

    # def send_close_signal(self, *args): # To prevent double closing 
    #     self.has_to_close = True

    def send_close_signal(self, *args): # To prevent double closing 
        print("BORRAR CLASE ACCEPTER SIGTERM")
        if not self.has_to_close:
            self.accepter_process.terminate()
            print("BORRAR Envie sigterm al accepter process")
        self.has_to_close = True
