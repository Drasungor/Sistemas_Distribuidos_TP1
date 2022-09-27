import json
from MOM.MOM import MOM

def print_line(line):
    for element in line:
        print(element, end = ",")
        print("")

def main():
    middleware = MOM("likes_filter_views_sum", print_line)
    middleware.start_received_messages_processing()

if __name__ == "__main__":
    main()