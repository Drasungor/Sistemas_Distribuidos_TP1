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
        self.connection_mode = connection_mode
        connections = local_config["connections"]
        if not (connection_mode in connections):
            raise ValueError(f"Connection mode is {connection_mode}, and should be one of the following: {connections.keys()}")

        if connection_mode == "accepter":
            self.sender = []
            connections_array = connections["accepter"]["sends_to"]
            self.sender.append((connections_array[0], config["general_aggregator"]["computers_amount"])) # (exchange name, receiver computers amount)
            self.sender.append((connections_array[1], config["likes_filter_views_sum"]["computers_amount"])) # (exchange name, receiver computers amount)

            self.channel.exchange_declare(exchange=self.sender[0][0], exchange_type='fanout')
            self.channel.exchange_declare(exchange=self.sender[1][0], exchange_type='fanout')

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

    def send_line(self, line):
        if self.connection_mode == "accepter":
            line_string = json.dumps(line)

            video_id = line[general_config["video_id_index"]]
            routing_key_number = hash(video_id) % self.sender[0][1]
            self.channel.basic_publish(exchange = self.sender[0][0], routing_key = str(routing_key_number), body = line_string)

            trending_date = line[general_config["trending_date_index"]]
            routing_key_number = hash(trending_date) % self.sender[1][1]
            self.channel.basic_publish(exchange = self.sender[1][0], routing_key = str(routing_key_number), body = line_string)

        elif self.connection_mode == "general_aggregator":
            pass
        elif self.connection_mode == "likes_filter_views_sum":
            pass
        elif self.connection_mode == "max_views_day":
            pass
        elif self.connection_mode == "likes_sum_funny_filter":
            pass
        elif self.connection_mode == "countries_amount_filter":
            pass
        else:
            # raise error
            pass

    def close(self):
        self.connection.close()