import json
from MOM.MOM import MOM

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config["repetition_filter"]

class ViewsSum:
    def __init__(self):
        self.middleware = MOM("views_sum", self.process_received_message)
        self.sent_videos = {}

    def process_received_message(self, ch, method, properties, body):
        line = json.loads(body)
        # # date: str = line[general_config["trending_date_index"]]
        # # view_count: str = line[general_config["views_index"]]
        # # if not (date in self.aggregation_dict):
        # #     self.aggregation_dict[date] = view_count
        # # else:
        # #     self.aggregation_dict[date] += view_count

        # if not video in set
            # insert in set
            # send video
        video_id = line[general_config["video_id_index"]]
        country = line[general_config["video_id_index"]]
        if not (video_id in self.videos_countries):
            self.videos_countries[video_id] = set()
        current_countries_amount = len(self.videos_countries[video_id])
        self.videos_countries[video_id].add()

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = ViewsSum()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()