import json
from MOM import MOM
import requests
import base64
import signal
import logging

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
        self.has_to_close = False
        self.previous_stage_size = self.middleware.get_previous_stage_size()

        signal.signal(signal.SIGTERM, self.__handle_signal)

    def process_received_message(self, ch, method, properties, body):
        if method.routing_key == general_config["general_subscription_routing_key"]:
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                self.has_to_close = True
        else:
            line = json.loads(body)
            video_id = line[local_config["indexes"]["video_id"]]
            thumbnail_link = line[local_config["indexes"]["thumbnail_link"]]
            img_data = requests.get(thumbnail_link).content
            self.middleware.send({ "type": cluster_type, "img_data": (video_id, base64.b64encode(img_data).decode()) })

        if self.has_to_close:
            self.middleware.send_general(None)
            self.middleware.close()
            print("Closed MOM")

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

    def __handle_signal(self, *args): # To prevent double closing 
        self.has_to_close = True


def main():
    # logging.basicConfig(
    #     format='%(asctime)s %(levelname)-8s %(message)s',
    #     level="DEBUG",
    #     datefmt='%Y-%m-%d %H:%M:%S',
    # )
    wrapper = MaxViewsDay()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()