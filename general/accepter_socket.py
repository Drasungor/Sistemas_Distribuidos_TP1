import socket
from communication_socket import CommunicationSocket

class AccepterSocket():
    def __init__(self):
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def bind_and_listen(self, port: int, pending_connections_amount: int):
        self.skt.bind(('', port))
        self.skt.listen(pending_connections_amount)

    def accept(self):
        return CommunicationSocket(self.skt.accept())

    def close(self):
        self.skt.close()