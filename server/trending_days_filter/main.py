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
        self.received_eofs = 0
        
        previous_stage = local_config["receives_from"]
        self.previous_stage_size = config[previous_stage]["computers_amount"]

    def process_received_message(self, ch, method, properties, body):
        if method.routing_key == general_config["general_subscription_routing_key"]:
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                self.middleware.send_general(None)
        else:
            line = json.loads(body)
            video_id = line[general_config["indexes"]["video_id"]]
            if not (video_id in self.trending_days_amounts):
                self.trending_days_amounts[video_id] = 0
            self.trending_days_amounts[video_id] += 1
            current_trending_days_amount = self.trending_days_amounts[video_id]
            if current_trending_days_amount == local_config["min_trending_days"]:
                self.middleware.send(line)

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = TrendingDaysFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()