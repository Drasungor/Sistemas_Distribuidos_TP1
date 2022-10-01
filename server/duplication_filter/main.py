import json
from MOM.MOM import MOM

cluster_type = "duplication_filter"

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config[cluster_type]

class DuplicationFilter:
    def __init__(self):
        self.middleware = MOM(cluster_type, self.process_received_message)
        self.sent_videos = set()
        self.received_eofs = 0
        
        previous_stage = local_config["receives_from"]
        self.previous_stage_size = config[previous_stage]["computers_amount"]

    def process_received_message(self, ch, method, properties, body):
        line = json.loads(body)

        if method.routing_key == general_config["EOF_subscription_routing_key"]:
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                self.middleware.send_final(None)
        else:
            video_id = line[general_config["indexes"]["video_id"]]
            title = line[general_config["indexes"]["title"]]
            category = line[general_config["indexes"]["category"]]
            if not (video_id in self.received_eofs):
                self.sent_videos.add(video_id)
                self.middleware.send({ "type": cluster_type, "tuple": (video_id, title, category) })

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = DuplicationFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()