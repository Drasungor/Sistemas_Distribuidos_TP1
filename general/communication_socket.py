import socket
import json

class ClosedSocket(Exception):
	pass

class CommunicationSocket():
    def __init__(self, skt = None):
        if skt is None:
            self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.skt = skt

    def connect(self):
        pass

    def __recv_all(self, bytes_amount: int):
            total_received_bytes = b''
            while (len(total_received_bytes) < bytes_amount):
                received_bytes = self.skt.recv(bytes_amount - len(total_received_bytes))
                if (len(received_bytes) == 0):
                    raise ClosedSocket
                total_received_bytes += received_bytes
            return total_received_bytes


    def __read_string(self):
        string_length = int.from_bytes(self.__recv_all(4), "big")
        read_string = self.__recv_all(string_length).decode()
        return read_string


    def read_json(self):
        return json.loads(self.__read_string())

    def send_number(self, number: int):
        self.skt.sendall(number.to_bytes(4, "big"))

    def _send_string(self, data: str):
        encoded_data = data.encode()
        message_length = len(encoded_data)
        self.skt.sendall(message_length.to_bytes(4, "big"))
        self.skt.sendall(data.encode())

    def send_json(self, data):
        self._send_string(json.dumps(data))
