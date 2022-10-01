import json
from MOM.MOM import MOM

cluster_type = "max_views_day"

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
                final_message_dict = { "type": cluster_type, "max_day": self.max_views_date }
                self.middleware.send(final_message_dict)
                self.middleware.send_general(None)
        else:
            daily_views_dict = json.loads(body)
            for day in daily_views_dict:
                if daily_views_dict[day] > self.max_views_date[1]:
                    self.max_views_date = (day, daily_views_dict[day])

    def start_received_messages_processing(self):
        self.middleware.start_received_messages_processing()

def main():
    wrapper = MaxViewsDay()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()