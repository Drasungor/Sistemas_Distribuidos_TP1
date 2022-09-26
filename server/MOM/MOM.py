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
        self.receiver = (None, None) # (exchange, queue)
        self.sender = [(None, None)] # [(exchange, connections_amount)]
        connections = local_config["connections"]
        if not (connection_mode in connections):
            raise ValueError(f"Connection mode is {connection_mode}, and should be one of the following: {connections.keys()}")

        if connection_mode == "accepter":
            self.sender = []
            connections_array = connections["accepter"]["sends_to"]
            self.sender.append((connections_array[0], config["general_aggregator"]["computers_amount"]))
            self.sender.append((connections_array[0], config["general_aggregator"]["computers_amount"]))

            # TODO: TERMINAR DE CONFIGURAR LAS COLAS DE PUBLISH Y SUBSCRIBE

            # TODO: assign receiver
                
        elif connection_mode == "general_aggregator":
            self.receiver = (connections["general_aggregator"]["receives_from"], "")
            # TODO: assign sender
        elif connection_mode == "likes_filter_views_sum":
            self.receiver = (connections["likes_filter_views_sum"]["receives_from"], "")
            # TODO: assign sender
        elif connection_mode == "max_views_day":
            pass
        elif connection_mode == "likes_sum_funny_filter":
            pass
        elif connection_mode == "countries_amount_filter":
            pass
        else:
            # raise error
            pass

    def close(self):
        self.connection.close()