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
        self.receiver = (None, None) # (exchange, queue)
        self.sender = [(None, None)] # [(exchange, connections_amount)] | 
        self.connection_mode = connection_mode
        connections = local_config["connections"]
        if not (connection_mode in connections):
            raise ValueError(f"Connection mode is {connection_mode}, and should be one of the following: {connections.keys()}")


        subscribes_to_keywords = False

        # TODO: HACER QUE LOS CONFIGS SE LLAMEN IGUAL QUE LAS COLAS RECEPTORAS, ASI NO HAY QUE HARDCODEAR LOS PROCESOS A LOS QUE SE ENVIAN COSAS (EJ CANTIDAD DE PCS),
        # IGUAL TAL VEZ NO TIENE MUCHO SENTIDO PORQUE IGUAL EL RUTEO POR HASHING DEPENDE DEL DESTINO, IGUAL ESO TAMBIEN PODRIA LLEGAR A SER CONFIGURABLE

        if connection_mode == "accepter":
            # Sending
            self.sender = []
            connections_array = connections["accepter"]["sends_to"]
            self.sender.append((connections_array[0], config["likes_filter"]["computers_amount"])) # (exchange name, receiver computers amount)
            self.sender.append((connections_array[1], config["trending_days_filter"]["computers_amount"])) # (exchange name, receiver computers amount)
            self.channel.exchange_declare(exchange = self.sender[0][0], exchange_type = "direct")
            self.channel.exchange_declare(exchange = self.sender[1][0], exchange_type = "direct")

            # Receiving
            self.receiver = ("", connections["accepter"]["receives_from"])
            queue_name = self.receiver[1]
            self.channel.queue_declare(queue = queue_name)
            self.channel.basic_consume(queue=queue_name, on_message_callback=receiver_callback, auto_ack=True)
                
        elif connection_mode == "funny_filter":
            # Sending
            self.sender = (connections["funny_filter"]["sends_to"], config["duplication_filter_input"]["computers_amount"]) # (exchange name, receiver computers amount)
            self.channel.exchange_declare(exchange = self.sender[0], exchange_type = "direct")

            # Receiving
            subscribes_to_keywords = True

        elif connection_mode == "likes_filter":
            # Sending
            self.sender = []
            connections_array = connections["likes_filter"]["sends_to"]
            self.sender.append((connections_array[0], config["funny_filter_input"]["computers_amount"])) # (exchange name, receiver computers amount)
            self.sender.append((connections_array[1], config["views_sum_input"]["computers_amount"])) # (exchange name, receiver computers amount)
            self.channel.exchange_declare(exchange = self.sender[0][0], exchange_type = "direct")
            self.channel.exchange_declare(exchange = self.sender[1][0], exchange_type = "direct")
            
            # Receiving
            subscribes_to_keywords = True

        elif connection_mode == "duplication_filter":
            # Sending
            self.sender = ("", connections["duplication_filter"]["sends_to"])
            self.channel.queue_declare(queue = self.sender[1])
            
            # Receiving
            subscribes_to_keywords = True

        elif connection_mode == "max_views_day":
            # Sending
            self.sender = ("", connections["max_views_day"]["sends_to"])
            self.channel.queue_declare(queue = self.sender[1])
            
            # Receiving
            self.receiver = ("", connections["max_views_day"]["receives_from"])
            queue_name = self.receiver[1]
            self.channel.queue_declare(queue = queue_name)
            self.channel.basic_consume(queue=queue_name, on_message_callback=receiver_callback, auto_ack=True)

        elif connection_mode == "views_sum":
            # Sending
            self.sender = ("", connections["views_sum"]["sends_to"])
            self.channel.queue_declare(queue = self.sender[1])
            
            # Receiving
            subscribes_to_keywords = True

        elif connection_mode == "trending_days_filter":
            # Sending
            self.sender = (connections["trending_days_filter"]["sends_to"], config["countries_amount_filter_input"]["computers_amount"]) # (exchange name, receiver computers amount)
            self.channel.exchange_declare(exchange = self.sender[0], exchange_type = "direct")

            # Receiving
            subscribes_to_keywords = True

        elif connection_mode == "countries_amount_filter":
            # Sending
            self.sender = ("", connections["countries_amount_filter"]["sends_to"])
            self.channel.queue_declare(queue = self.sender[1])
            
            # Receiving
            subscribes_to_keywords = True

        elif connection_mode == "thumbnails_downloader":
            # Sending
            self.sender = ("", connections["thumbnails_downloader"]["sends_to"])
            self.channel.queue_declare(queue = self.sender[1])
            
            
            # Receiving
            subscribes_to_keywords = True

        else:
            # raise error
            pass

        if subscribes_to_keywords:
            self.channel.exchange_declare(exchange = connections[connection_mode]["receives_from"], exchange_type = "direct")
            self.receiver = (connections[connection_mode]["receives_from"], "")
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            self.channel.queue_bind(exchange = self.receiver[0], queue = queue_name, routing_key = os.environ["NODE_ID"])
            self.channel.queue_bind(exchange = self.receiver[0], queue = queue_name, routing_key = general_config["EOF_subscription_routing_key"])
            self.channel.basic_consume(queue=queue_name, on_message_callback=receiver_callback, auto_ack=True)
        else:
            pass

    def send(self, message):
        message_string = json.dumps(message)
        if self.connection_mode == "accepter":
            line = message
            video_id = line[general_config["video_id_index"]]
            routing_key_number = hash(video_id) % self.sender[0][1]
            self.channel.basic_publish(exchange = self.sender[0][0], routing_key = str(routing_key_number), body = message_string)

            trending_date = line[general_config["trending_date_index"]]
            routing_key_number = hash(trending_date) % self.sender[1][1]
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