import json
from MOM import MOM
import signal
import logging

cluster_type = "likes_filter"

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config[cluster_type]

class LikesFilter:
    def __init__(self):
        self.middleware = MOM(cluster_type, self.process_received_line)
        self.received_eofs = 0
        
        self.has_to_close = False
        self.is_processing_message = False

        previous_stage = local_config["receives_from"]
        if previous_stage == "accepter":
            self.previous_stage_size = config[previous_stage]["processes_amount"]
        else:
            self.previous_stage_size = config[previous_stage]["computers_amount"]

        signal.signal(signal.SIGTERM, self.__handle_signal)

    def process_received_line(self, ch, method, properties, body):
        # logging.info("BORRAR Voy a procesar un mensaje")
        # print("BORRAR Voy a procesar un mensaje")
        self.is_processing_message = True
        line = json.loads(body)
        if method.routing_key == general_config["general_subscription_routing_key"]:
            if line == None:
                self.received_eofs += 1
                if self.received_eofs == self.previous_stage_size:
                    self.middleware.send_general(None)
                    # self.middleware.close()
                    self.has_to_close = True
                else:
                    self.middleware.send_general(line)
        else:
            likes_amount: str = line[local_config["indexes"]["likes"]]
            if int(likes_amount) >= local_config["likes_min"]:
                self.middleware.send_line(line)

        if self.has_to_close:
            logging.info("BOORRAR VOY A CERRAR MOM")
            self.middleware.close()
            logging.info("Closed MOM")
        self.is_processing_message = False

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

    def __handle_signal(self, *args): # To prevent double closing 
        if self.is_processing_message:
            self.has_to_close = True
        else:
            # logging.info("BOORRAR VOY A CERRAR MOM")
            print("BOORRAR VOY A CERRAR MOM")
            self.middleware.close()
            # logging.info("Closed MOM")
            print("Closed MOM")


def main():
    # print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    # logging.debug("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    wrapper = LikesFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()