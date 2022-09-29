import json
import os
from MOM.MOM import MOM

def print_line(ch, method, properties, body):
    line = json.loads(body)
    for element in line:
        id = os.environ["NODE_ID"]
        print(f"Node id {id}: {element}", end = ",")
        print("")

def main():
    middleware = MOM("general_aggregator", print_line)
    middleware.start_received_messages_processing()

if __name__ == "__main__":
    main()