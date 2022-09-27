import json
from MOM.MOM import MOM
import os

def print_line(ch, method, properties, body): 
    for element in body:
        id = os.environ["NODE_ID"]
        print(f"Node id {id}: {element}", end = ",")
        print("")

def main():
    middleware = MOM("likes_filter_views_sum", print_line)
    middleware.start_received_messages_processing()

if __name__ == "__main__":
    main()