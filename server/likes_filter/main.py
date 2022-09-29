import json
from MOM.MOM import MOM

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config["likes_filter"]

class FunnyFilter:
    def __init__(self):
        self.middleware = MOM("likes_filter", self.process_received_line)

    def process_received_line(self, ch, method, properties, body):
        line = json.loads(body)
        likes_amount: str = line[general_config["likes_index"]]
        if likes_amount >= local_config["likes_min"]:
            self.middleware.send(body)

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = FunnyFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()