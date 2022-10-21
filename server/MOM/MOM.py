import json
import pika
import hashlib
import os

config_file_path = "/config/config.json"
config = None
with open(config_file_path, "r") as config_file:
    config = json.load(open(config_file_path, "r"))
general_config = config["general"]
local_config = config["MOM"]

class MOM:
    def __init__(self, connection_mode: str, receiver_callback):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(local_config["broker_address"]))
        self.channel = self.connection.channel()
        self.receiver = None # exchange | queue
        self.sender = None # [(exchange name, [hashing attributes], connections_amount)] | [queue name]
        self.connection_mode = None
        self.queue_tag = None
        self.previous_stage_size = None
        connections = local_config["connections"]
        connections_names = list(connections.keys())
        connections_names.append("accepter_sender")
        if not (connection_mode in connections_names):
            raise ValueError(f"Connection mode is {connection_mode}, and should be one of the following: {connections_names}")

        receives_messages = True

        subscribes_to_keywords = False # reads from publisher/subscriber
        self.sends_to_publisher = False # sends to publisher/subscriber

        if connection_mode == "accepter" or connection_mode == "accepter_sender":
            if connection_mode == "accepter_sender":
                receives_messages = False

            connection_mode = "accepter"
            # Sending
            self.sends_to_publisher = True

            # Receiving

            # subscribes_to_keywords is already false
            self.previous_stage_size = 0
            for previous_stage in config[connection_mode]["receives_from"]:
                self.previous_stage_size += config[previous_stage]["computers_amount"]

        elif  connection_mode in ["funny_filter", "likes_filter", "countries_amount_filter", "trending_days_filter"]:
            # Sending
            self.sends_to_publisher = True

            # Receiving
            subscribes_to_keywords = True

        elif connection_mode in ["duplication_filter", "views_sum", "thumbnails_downloader"]:
            # Sending
            # sends_to_publisher is already false
            
            # Receiving
            subscribes_to_keywords = True

        elif connection_mode != "max_views_day":
            # If connection_mode == "max_views_day" then
            # Sending
            # sends_to_publisher is already false
            
            # Receiving
            # subscribes_to_keywords is already false

            raise ValueError(f"Unexpected connection mode {connection_mode}, it should be one of the following: {connections.keys()}")

        self.connection_mode = connection_mode

        if self.previous_stage_size == None:
            previous_stage = config[connection_mode]["receives_from"]
            if previous_stage == "accepter":
                self.previous_stage_size = config[previous_stage]["processes_amount"]
            else:
                self.previous_stage_size = config[previous_stage]["computers_amount"]

        if receives_messages:
            if subscribes_to_keywords:
                self.channel.exchange_declare(exchange = connection_mode, exchange_type = "direct")
                self.receiver = connection_mode
                result = self.channel.queue_declare(queue='', exclusive=True)
                queue_name = result.method.queue
                self.channel.queue_bind(exchange = self.receiver, queue = queue_name, routing_key = os.environ["NODE_ID"])
                self.channel.queue_bind(exchange = self.receiver, queue = queue_name, routing_key = general_config["general_subscription_routing_key"])
                self.queue_tag = self.channel.basic_consume(queue = queue_name, on_message_callback=receiver_callback, auto_ack=True)
            else:
                self.receiver = connection_mode
                self.channel.queue_declare(queue = connection_mode)
                self.queue_tag = self.channel.basic_consume(queue = connection_mode, on_message_callback=receiver_callback, auto_ack=True)

        if self.sends_to_publisher:
            self.sender = []
            connections_array = connections[connection_mode]["sends_to"]
            for connecting_to in connections_array:
                self.sender.append((connecting_to, config[connecting_to]["hashed_by"], config[connecting_to]["computers_amount"])) # (exchange name, [hashing attributes], receiver computers amount)
                self.channel.exchange_declare(exchange = connecting_to, exchange_type = "direct")
        else:
            self.sender = []
            for connection in connections[connection_mode]["sends_to"]:
                self.sender.append(connection)
                self.channel.queue_declare(queue = connection)


    def __get_hashing_key(self, line, receiving_end, hashing_attributes):
        indexes_object = config[receiving_end]["indexes"]
        hashing_attributes_iterator = iter(hashing_attributes)
        hashing_string = line[indexes_object[next(hashing_attributes_iterator)]]
        for hashing_attribute in hashing_attributes_iterator: # Extends to more than 2 receiving ends
            aux = line[indexes_object[hashing_attribute]]
            hashing_string += f"-{aux}"
        return hashing_string

    def send(self, message):
        message_string = json.dumps(message)
        if self.sends_to_publisher:
            for receiving_end in self.sender:
                line = message
                hashing_attributes = receiving_end[1]
                hashing_string = self.__get_hashing_key(line, receiving_end[0], hashing_attributes)

                routing_key_number = int(hashlib.sha512(hashing_string.encode()).hexdigest(), 16) % receiving_end[2]

                self.channel.basic_publish(exchange = receiving_end[0], routing_key = str(routing_key_number), body = message_string)
        else:
            for receiving_end in self.sender:
                self.channel.basic_publish(exchange = "", routing_key = receiving_end, body = message_string)

    def get_previous_stage_size(self):
        return self.previous_stage_size

    def send_line(self, line):
        kept_attributes = config[self.connection_mode]["kept_columns"]
        indexes = config[self.connection_mode]["indexes"]
        kept_attributes = list(map(lambda column: indexes[column], kept_attributes))
        kept_attributes.sort()
        sent_line = []
        for index in kept_attributes:
            sent_line.append(line[index])
        self.send(sent_line)

    def send_general(self, message):
        message_string = json.dumps(message)
        if self.sends_to_publisher:
            for receiving_end in self.sender:
                self.channel.basic_publish(exchange = receiving_end[0], routing_key = general_config["general_subscription_routing_key"], body = message_string)
        else:
            for receiving_end in self.sender:
                self.channel.basic_publish(exchange = "", routing_key = receiving_end, body = message_string)

    def start_received_messages_processing(self):
        self.channel.start_consuming()

    def close(self):
        if self.connection != None:
            self.channel.basic_cancel(self.queue_tag)
            self.connection.close()
            self.connection = None