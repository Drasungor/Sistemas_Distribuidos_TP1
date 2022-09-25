import json
import pika

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config["MOM"]

class MOM:
    def __init__(self, connection_mode: str):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(local_config["broker_address"]))
        self.channel = self.connection.channel()

        connections = local_config["connections"]
        if not (connection_mode in connections):
            raise ValueError(f"Connection mode is {connection_mode}, and should be one of the following: {connections.keys()}")

        if connection_mode == "accepter":
            pass
        elif connection_mode == "general_aggregator":
            pass
        elif connection_mode == "likes_filter_views_sum":
            pass
        elif connection_mode == "max_views_day":
            pass
        elif connection_mode == "likes_sum_funny_filter":
            pass
        elif connection_mode == "countries_amount_filter":
            pass
        else:
            # raise error
            pass