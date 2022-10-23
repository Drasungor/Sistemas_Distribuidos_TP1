import signal

class SigtermNotifier:
    def __init__(self, processes = None):
        self.received_sigterm = False
        self.processes = processes
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

    def __handle_sigterm(self, *args):
        self.received_sigterm = True
        if self.processes != None:
            for process in self.processes:
                process.terminate()
