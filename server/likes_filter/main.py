import json
from MOM import MOM
import time

# time.sleep(60)

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
        
        previous_stage = local_config["receives_from"]
        if previous_stage == "accepter":
            self.previous_stage_size = config[previous_stage]["processes_amount"]
        else:
            self.previous_stage_size = config[previous_stage]["computers_amount"]

    def process_received_line(self, ch, method, properties, body):
        line = json.loads(body)
        if method.routing_key == general_config["general_subscription_routing_key"]:
        # if line == None:
            self.received_eofs += 1
            print(f"Current received eofs: {self.received_eofs}")
            if self.received_eofs == self.previous_stage_size:
                print(f"VOY A ENVIAR NONE, received eofs: {self.received_eofs}, expected eofs: {self.previous_stage_size}")
                self.middleware.send_general(None)
        else:
            # print(line)
            likes_amount: str = line[general_config["indexes"]["likes"]]
            if int(likes_amount) >= local_config["likes_min"]:
                self.middleware.send(line)

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = LikesFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()