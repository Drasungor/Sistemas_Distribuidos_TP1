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
        self.sender = [(None, None)] # [(exchange, connections_amount)] | 
        self.connection_mode = connection_mode
        connections = local_config["connections"]
        if not (connection_mode in connections):
            raise ValueError(f"Connection mode is {connection_mode}, and should be one of the following: {connections.keys()}")


        subscribes_to_keywords = False # reads from publisher/subscriber
        sends_to_publisher = False # sends to publisher/subscriber

        # TODO: HACER QUE LOS CONFIGS SE LLAMEN IGUAL QUE LAS COLAS RECEPTORAS, ASI NO HAY QUE HARDCODEAR LOS PROCESOS A LOS QUE SE ENVIAN COSAS (EJ CANTIDAD DE PCS),
        # IGUAL TAL VEZ NO TIENE MUCHO SENTIDO PORQUE IGUAL EL RUTEO POR HASHING DEPENDE DEL DESTINO, IGUAL ESO TAMBIEN PODRIA LLEGAR A SER CONFIGURABLE

        if connection_mode == "accepter":
            # Sending
            sends_to_publisher = True

            # Receiving
            # subscribes_to_keywords is already false
                
        elif  connection_mode in ["funny_filter", "likes_filter", "trending_days_filter"]:
            # Sending
            sends_to_publisher = True

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

        if sends_to_publisher:
            self.sender = []
            connections_array = connections[connection_mode]["sends_to"]
            for connecting_to in connections_array:
                self.sender.append((connecting_to, config[connecting_to]["computers_amount"])) # (exchange name, receiver computers amount)
                self.channel.exchange_declare(exchange = connecting_to, exchange_type = "direct")
        else:
            self.sender = (connections[connection_mode]["sends_to"], "")
            self.channel.queue_declare(queue = self.sender[0])

    def send(self, message):
        message_string = json.dumps(message)
        if self.connection_mode == "accepter":
            line = message
            video_id = line[general_config["video_id_index"]]
            routing_key_number = hash(video_id) % self.sender[0][1] # It would still work with a task queue, but it is more easily configurable if everything
                                                                    # is an exchange, and also we don't leak implementation specifications into the config file
                                                                    # TODO: set hashing keys in config
            self.channel.basic_publish(exchange = self.sender[0][0], routing_key = str(routing_key_number), body = message_string)

            country = line[general_config["country_index"]]
            hashing_string = f"{video_id}-{country}"
            routing_key_number = hash(hashing_string) % self.sender[1][1]
            self.channel.basic_publish(exchange = self.sender[1][0], routing_key = str(routing_key_number), body = message_string)
        elif self.connection_mode == "funny_filter":
            pass
        elif self.connection_mode == "likes_filter":
            pass
        elif self.connection_mode == "duplication_filter":
            pass
        elif self.connection_mode == "max_views_day":
            pass
        elif self.connection_mode == "views_sum":
            pass
        elif self.connection_mode == "trending_days_filter":
            pass
        elif self.connection_mode == "countries_amount_filter":
            pass
        elif self.connection_mode == "thumbnails_downloader":
            pass
        else:
            # raise error
            pass

    def send_final(self, message):
        pass

    def start_received_messages_processing(self):
        self.channel.start_consuming()

    def close(self):
        self.connection.close()