import json
from MOM.MOM import MOM

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config["countries_amount"]

class CountriesAmountFilter:
    def __init__(self):
        self.middleware = MOM("countries_amount_filter", self.process_received_message)
        self.videos_countries = {}
        self.countries_amount = None # TODO: we should receive the amount of countries after the client establishes a connection

    def process_received_message(self, ch, method, properties, body):
        line = json.loads(body)


        if method.routing_key == general_config["EOF_subscription_routing_key"]:
            # TODO: IMPLEMENT THIS
            pass
        else:
            video_id = line[general_config["indexes"]["video_id"]]
            country = line[general_config["indexes"]["country"]]
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