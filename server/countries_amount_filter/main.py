import json
from MOM import MOM
import signal
import logging

cluster_type = "countries_amount_filter"

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config[cluster_type]

class CountriesAmountFilter:
    def __init__(self):
        self.middleware = MOM(cluster_type, self.process_received_message)
        self.videos_countries = {}
        self.countries_amount = None
        self.received_eofs = 0
        self.has_to_close = False
        self.previous_stage_size = self.middleware.get_previous_stage_size()

        signal.signal(signal.SIGTERM, self.__handle_signal)

    def process_received_message(self, ch, method, properties, body):
        line = json.loads(body)
        if method.routing_key == general_config["general_subscription_routing_key"]:
            if line == None:
                self.received_eofs += 1
                if self.received_eofs == self.previous_stage_size:
                    self.has_to_close = True
            else:
                self.countries_amount = line # Number
        else:
            video_id = line[local_config["indexes"]["video_id"]]
            country = line[local_config["indexes"]["country"]]
            if not (video_id in self.videos_countries):
                self.videos_countries[video_id] = set()
            video_set = self.videos_countries[video_id]
            previous_countries_amount = len(video_set)
            video_set.add(country)
            current_countries_amount = len(video_set)
            if (current_countries_amount == self.countries_amount) and (previous_countries_amount != current_countries_amount):
                self.middleware.send(line)

        if self.has_to_close:
            self.middleware.send_general(None)
            self.middleware.close()
            print("Closed MOM")


    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()
        self.has_to_close = True

    def __handle_signal(self, *args): # To prevent double closing 
        self.has_to_close = True

def main():
    # logging.basicConfig(
    #     format='%(asctime)s %(levelname)-8s %(message)s',
    #     level="DEBUG",
    #     datefmt='%Y-%m-%d %H:%M:%S',
    # )
    wrapper = CountriesAmountFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()