import json
from MOM import MOM

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
        
        previous_stage = local_config["receives_from"]
        self.previous_stage_size = config[previous_stage]["computers_amount"]

    def process_received_message(self, ch, method, properties, body):
        line = json.loads(body)
        if method.routing_key == general_config["general_subscription_routing_key"]:
        # if line == None:
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                print(f"VOY A ENVIAR NONE, received eofs: {self.received_eofs}, expected eofs: {self.previous_stage_size}")
                self.middleware.send_general(self.aggregation_dict)
                self.middleware.send_general(None)
                self.middleware.close()
        else:
            # date: str = line[general_config["indexes"]["trending_date"]]
            # view_count: int = int(line[general_config["indexes"]["views"]])
            date: str = line[local_config["indexes"]["trending_date"]]
            view_count: int = int(line[local_config["indexes"]["views"]])
            if not (date in self.aggregation_dict):
                self.aggregation_dict[date] = view_count
            else:
                self.aggregation_dict[date] += view_count

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = ViewsSum()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()