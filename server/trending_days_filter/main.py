import json
from MOM import MOM

cluster_type = "trending_days_filter"

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config[cluster_type]

class TrendingDaysFilter:
    def __init__(self):
        self.middleware = MOM(cluster_type, self.process_received_message)
        self.trending_days_amounts = {}
        self.trending_days_amounts_aux = {}
        self.received_eofs = 0
        
        previous_stage = local_config["receives_from"]
        if previous_stage == "accepter":
            self.previous_stage_size = config[previous_stage]["processes_amount"]
        else:
            self.previous_stage_size = config[previous_stage]["computers_amount"]

    def process_received_message(self, ch, method, properties, body):
        if method.routing_key == general_config["general_subscription_routing_key"]:
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                self.middleware.send_general(None)
                self.middleware.close()
        else:
            line = json.loads(body)
            video_id = line[local_config["indexes"]["video_id"]]
            country = line[local_config["indexes"]["country"]]
            key = f"{video_id}-{country}"
            if not (key in self.trending_days_amounts):
                self.trending_days_amounts_aux[key] = 0
                self.trending_days_amounts[key] = set()
            self.trending_days_amounts_aux[key] += 1
            current_video_set = self.trending_days_amounts[key]
            previous_trending_days_amount = len(self.trending_days_amounts[key])
            self.trending_days_amounts[key].add(line[local_config["indexes"]["trending_date"]])
            current_trending_days_amount = len(self.trending_days_amounts[key])
            if current_trending_days_amount == local_config["min_trending_days"] and current_trending_days_amount != previous_trending_days_amount:
                self.middleware.send_line(line)

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = TrendingDaysFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()