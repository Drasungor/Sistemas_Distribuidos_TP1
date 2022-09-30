import json
from MOM.MOM import MOM

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config["funny_filter"]

class FunnyFilter:
    def __init__(self):
        self.middleware = MOM("funny_filter", self.process_received_line)

    def process_received_line(self, ch, method, properties, body):
        line = json.loads(body)
        tags: str = line[general_config["indexes"]["tags"]]
        if local_config["tag"] in tags:
            self.middleware.send(body)

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = FunnyFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()