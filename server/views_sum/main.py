import json
from MOM import MOM
import signal
import logging

cluster_type = "views_sum"

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config[cluster_type]

class ViewsSum:
    def __init__(self):
        self.middleware = MOM(cluster_type, self.process_received_message)
        self.aggregation_dict = {}
        self.received_eofs = 0
        
        self.has_to_close = False
        # self.is_processing_message = False

        previous_stage = local_config["receives_from"]
        self.previous_stage_size = config[previous_stage]["computers_amount"]

        signal.signal(signal.SIGTERM, self.__handle_signal)

    def process_received_message(self, ch, method, properties, body):
        # self.is_processing_message = True
        line = json.loads(body)
        if method.routing_key == general_config["general_subscription_routing_key"]:
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                self.middleware.send_general(self.aggregation_dict)
                self.middleware.send_general(None)
                # self.middleware.close()
                self.has_to_close = True
        else:
            date: str = line[local_config["indexes"]["trending_date"]]
            view_count: int = int(line[local_config["indexes"]["views"]])
            if not (date in self.aggregation_dict):
                self.aggregation_dict[date] = view_count
            else:
                self.aggregation_dict[date] += view_count

        if self.has_to_close:
            self.middleware.close()
            logging.info("Closed MOM")
        # self.is_processing_message = False

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

    def __handle_signal(self, *args): # To prevent double closing 
        self.has_to_close = True
        # if self.is_processing_message:
        #     self.has_to_close = True
        # else:
        #     self.middleware.close()
        #     logging.info("Closed MOM")


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level="DEBUG",
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    wrapper = ViewsSum()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()