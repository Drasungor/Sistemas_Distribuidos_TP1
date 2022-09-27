config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config["accepter"]

class VideoData:
    def __init__(self):
        self.likes_acummulator = 0
        self.entries_amount = 0
        self.countries_appearances = set()

class Aggregator:
    def __init__(self):
        self.videos_dict = {} # dict[video_id] = (likes_sum, entries_sum, countries_set)

    def update_video_data(self, video_data):
        video_id = video_data[general_config["video_id_index"]]
        if video_id in self.videos_dict:
            pass
        else:
            pass