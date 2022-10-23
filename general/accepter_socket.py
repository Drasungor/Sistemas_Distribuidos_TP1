import socket
from communication_socket import CommunicationSocket

class AccepterSocket():
    def __init__(self, port: int, pending_connections_amount: int):
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.skt.bind(('', port))
        self.skt.listen(pending_connections_amount)
        self.has_closed = False

    def accept(self):
        skt, _ = self.skt.accept()
        # skt, asdasdasd = self.skt.accept()
        # print(f"BORRAR De la conexion lei {skt} y {asdasdasd}")
        return CommunicationSocket(skt)

    def close(self):
        if not self.has_closed:
            self.skt.close()
        self.has_closed = True