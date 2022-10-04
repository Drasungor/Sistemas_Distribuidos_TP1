import json
from MOM import MOM

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
        self.countries_amount = None # TODO: we should receive the amount of countries after the client establishes a connection
        self.received_eofs = 0
        
        previous_stage = local_config["receives_from"]
        self.previous_stage_size = config[previous_stage]["computers_amount"]

    def process_received_message(self, ch, method, properties, body):
        line = json.loads(body)
        if method.routing_key == general_config["general_subscription_routing_key"]:
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                self.middleware.send_general(None)
                self.middleware.close()
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
                self.middleware.send(video_id)

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = CountriesAmountFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()