import json
from MOM.MOM import MOM

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config["likes_filter"]

class LikesFilter:
    def __init__(self):
        self.middleware = MOM("likes_filter", self.process_received_line)
        self.received_eofs = 0
        
        previous_stage = local_config["receives_from"]
        self.previous_stage_size = config[previous_stage]["computers_amount"]

    def process_received_line(self, ch, method, properties, body):
        line = json.loads(body)
        if method.routing_key == general_config["EOF_subscription_routing_key"]:
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                self.middleware.send_final(body)
        else:
            likes_amount: str = line[general_config["indexes"]["likes"]]
            if likes_amount >= local_config["likes_min"]:
                self.middleware.send(body)

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = LikesFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()