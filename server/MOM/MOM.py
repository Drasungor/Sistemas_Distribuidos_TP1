import json
import pika
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
        self.connection_mode = connection_mode
        connections = local_config["connections"]
        if not (connection_mode in connections):
            raise ValueError(f"Connection mode is {connection_mode}, and should be one of the following: {connections.keys()}")


        subscribes_to_keywords = False # reads from publisher/subscriber
        self.sends_to_publisher = False # sends to publisher/subscriber

        # TODO: HACER QUE LOS CONFIGS SE LLAMEN IGUAL QUE LAS COLAS RECEPTORAS, ASI NO HAY QUE HARDCODEAR LOS PROCESOS A LOS QUE SE ENVIAN COSAS (EJ CANTIDAD DE PCS),
        # IGUAL TAL VEZ NO TIENE MUCHO SENTIDO PORQUE IGUAL EL RUTEO POR HASHING DEPENDE DEL DESTINO, IGUAL ESO TAMBIEN PODRIA LLEGAR A SER CONFIGURABLE

        if connection_mode == "accepter":
            # Sending
            self.sends_to_publisher = True

            # Receiving
            # subscribes_to_keywords is already false
                
        elif  connection_mode in ["funny_filter", "likes_filter", "trending_days_filter"]:
            # Sending
            self.sends_to_publisher = True

            # Receiving
            subscribes_to_keywords = True

        elif connection_mode in ["duplication_filter", "views_sum", "countries_amount_filter", "thumbnails_downloader"]:
            # Sending
            # sends_to_publisher is already false
            
            # Receiving
            subscribes_to_keywords = True

        elif connection_mode != "max_views_day":
            # Sending
            # sends_to_publisher is already false
            
            # Receiving
            # subscribes_to_keywords is already false

            raise ValueError(f"Unexpected connection mode {connection_mode}, it should be one of the following: {connections.keys()}")

        if subscribes_to_keywords:
            self.channel.exchange_declare(exchange = connection_mode, exchange_type = "direct")
            self.receiver = connection_mode
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            self.channel.queue_bind(exchange = self.receiver[0], queue = queue_name, routing_key = os.environ["NODE_ID"])
            self.channel.queue_bind(exchange = self.receiver[0], queue = queue_name, routing_key = general_config["EOF_subscription_routing_key"])
            self.channel.basic_consume(queue = queue_name, on_message_callback=receiver_callback, auto_ack=True)
        else:
            self.receiver = connection_mode
            self.channel.queue_declare(queue = connection_mode)
            self.channel.basic_consume(queue = connection_mode, on_message_callback=receiver_callback, auto_ack=True)

        if self.sends_to_publisher:
            self.sender = []
            connections_array = connections[connection_mode]["sends_to"]
            for connecting_to in connections_array:
                self.sender.append((connecting_to, config[connecting_to]["hashed_by"], config[connecting_to]["computers_amount"])) # (exchange name, [hashing attributes], receiver computers amount)
                self.channel.exchange_declare(exchange = connecting_to, exchange_type = "direct")
        else:
            self.sender = (connections[connection_mode]["sends_to"], "")
            for connection in connections[connection_mode]["sends_to"]:
                self.sender.append(connection)
                self.channel.queue_declare(queue = connection)


    def __get_hashing_key(self, line, hashing_attributes):
        indexes_object = general_config["indexes"]
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
                hashing_attributes = receiving_end[1] # TODO: this should be changed for when attributes are dropped between pipeline stages
                hashing_string = self.__get_hashing_key(line, hashing_attributes)
                routing_key_number = hash(hashing_string) % self.sender[0][1]
                self.channel.basic_publish(exchange = receiving_end[0], routing_key = str(routing_key_number), body = message_string)
        else:
            for receiving_end in self.sender:
                self.channel.basic_publish(exchange = "", routing_key = receiving_end, body = message_string)


    def send_final(self, message):
        message_string = json.dumps(message)
        if self.sends_to_publisher:
            for receiving_end in self.sender:
                self.channel.basic_publish(exchange = receiving_end[0], routing_key = general_config["EOF_subscription_routing_key"], body = message_string)
        else:
            for receiving_end in self.sender:
                self.channel.basic_publish(exchange = "", routing_key = receiving_end, body = message_string)

    def start_received_messages_processing(self):
        self.channel.start_consuming()

    def close(self):
        self.connection.close()