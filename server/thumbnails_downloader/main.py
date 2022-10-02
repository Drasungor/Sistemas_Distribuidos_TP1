import json
from MOM import MOM
import requests
import base64

cluster_type = "thumbnails_downloader"

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config[cluster_type]

class MaxViewsDay:
    def __init__(self):
        self.middleware = MOM(cluster_type, self.process_received_message)
        self.max_views_date = (None, 0)
        self.received_eofs = 0
        
        previous_stage = local_config["receives_from"]
        self.previous_stage_size = config[previous_stage]["computers_amount"]

    def process_received_message(self, ch, method, properties, body):
        if method.routing_key == general_config["general_subscription_routing_key"]: # TODO: check if this condition is correct
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                self.middleware.send_general(None)
        else:
            line = json.loads(body)
            video_id = line[general_config["indexes"]["video_id"]]
            img_data = requests.get(f"https://img.youtube.com/vi/{video_id}/0.jpg").content
            self.middleware.send({ "type": cluster_type, "img_data": (video_id, base64.b64encode(img_data)) })

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = MaxViewsDay()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()