import json
from MOM.MOM import MOM

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config["max_views_day"]

class MaxViewsDay:
    def __init__(self):
        self.middleware = MOM("views_sum", self.process_received_message)
        self.max_views_date = (None, 0)

    def process_received_message(self, ch, method, properties, body):
        # TODO: implement this logic
        # iterate received array and update max day
        # If cant finished received = pcs views sum amount
        # send max day
        if method.routing_key == "-1": # TODO: check if this condition is correct
            # Send current max
            # Check if also the last views sum is sent in this message
            final_message_dict = { "type": "max_views_day", "max_day": self.max_views_date }
            self.middleware.send_final(final_message_dict)
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