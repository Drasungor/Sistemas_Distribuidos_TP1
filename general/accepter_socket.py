import socket
from communication_socket import CommunicationSocket

class AccepterSocket():
    def __init__(self, port: int, pending_connections_amount: int):
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.skt.bind(('', port))
        self.skt.listen(pending_connections_amount)

    def accept(self):
        skt, _ = self.skt.accept()
        return CommunicationSocket(skt)

    def close(self):
        self.skt.close()