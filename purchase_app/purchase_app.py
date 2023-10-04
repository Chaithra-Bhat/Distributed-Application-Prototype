import csv
import http.server
import socketserver
import json
import threading
import os
import requests
import argparse
import re
from threading import Lock
from queue import Queue


hostName = None
# defines the port for the service
PORT = None

# Unique Id number of each replica of this service
INSTANCE_ID = None

LEADER_ID = None
ALL_ORDER_NODES = None
LEADER_HOST = None
LEADER_PORT = None

# initialize transaction number counter to 0
txn_num = 0
# list of order logs
memory_data = []
lock = Lock()

def append_to_memory_data(data):
    # This appends the order details to the memory data in a thread safe manner
    global memory_data
    print("Acquire lock")
    lock.acquire()
    memory_data.append(data)
    lock.release()

def extend_memory_data(data_li):
    # This extends the order details to the memory data in a thread safe manner
    global memory_data
    print("Acquire lock")
    lock.acquire()
    memory_data.extend(data_li)
    lock.release()

def search_txn_mem_data(txn_num_to_search):
    # This returns the index of the transaction number to be searched in the memory data in a thread safe manner
    follower_last_txn_index = None
    lock.acquire()
    print(f"Searching for transaction number {txn_num_to_search} in memory data")
    for i in range(len(memory_data)):
        mem_data_num = int(memory_data[i]["Transaction number"])
        print(f"Comparing with txn number from mem data {mem_data_num}")
        if int(txn_num_to_search) == int(mem_data_num):
            follower_last_txn_index = i
            break
    lock.release()
    return follower_last_txn_index


def get_last_txn_number():
    # This gets the most recent transaction number from the memory data in a thread safe manner
    global memory_data
    lock.acquire()
    if memory_data:
        val = memory_data[-1]['Transaction number']
        lock.release()
        return val
    else:
        lock.release()
        return None

def update_txn_number(updated_txn_num_val):
    # This updates the value of the transaction number counter in a thread safe manner
    global txn_num
    print(f"Txn num is {txn_num}, updating it to {updated_txn_num_val}")
    lock.acquire()
    txn_num = int(updated_txn_num_val)
    lock.release()
    return

def get_next_txn_number():
    # This returns the transaction number to be assigned to the next incoming order in a thread safe manner 
    global txn_num
    lock.acquire()
    txn_num += 1
    lock.release()
    return txn_num

def write_mem_data_to_file(filepath):
    # This writes the entire memory data to the order log file in a thread safe manner
    if memory_data:
        # a write lock is implemented which only one request can access at a time
        lock.acquire()
        with open(filepath, 'w', newline = '') as file:
            write = csv.DictWriter(file, fieldnames=['Transaction number', 'name', 'order type', 'quantity'])
            write.writeheader()
            for detail in memory_data:
                write.writerow(detail)
        lock.release()


class Order(http.server.BaseHTTPRequestHandler):

    # Like in catalog, an RLock is used in order service
    def __init__(self, *args, **kwargs):
        self.lock = threading.RLock()
        global LEADER_ID
        global ALL_ORDER_NODES
        global LEADER_HOST
        global LEADER_PORT
        global memory_data

        # Creating the path to the order service for the particular Instance ID
        self.order_log = os.path.join(os.getcwd(), "purchase_app", "resources", f"order_log_{INSTANCE_ID}.csv")

        # the in memory data structure useful for updating the log

        # defines the port and host name for the catalog service
        self.catalog_port = os.getenv("CATALOG_PORT")
        self.catalog_name = os.getenv("CATALOG_HOSTNAME")

        super().__init__(*args, **kwargs)

    def calculate_txn_number(self):
        # This calls the global function that returns the transaction number to be assigned to the next incoming order
        transaction_number = get_next_txn_number()
        print(f"Transaction number: {transaction_number}")
        return transaction_number


    def do_GET(self):
        global memory_data
        # verifies that the path of the front-end service is correct
        if self.path.startswith("/query/"):
            # Queries the orders
            order_no = self.path.split("/query/")[1]

            # a read lock for every read request
            with self.lock:
                for data in memory_data:

                    # check if the number received is valid
                    if str(data["Transaction number"]) == order_no:
                        request_response = {
                            "number" : data["Transaction number"],
                            "name" : data["name"],
                            "type" : data["order type"],
                            "quantity" : data["quantity"]
                        }

                        # successful response sent back
                        final_response = json.dumps({"data" : request_response})
                        self.send_response(200)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(final_response.encode("utf-8"))
                        return

            # error message for invalid number
            fail_response = {
                "code" : 404,
                "message" : "invalid number"
            }

            # error message sent back
            final_message = json.dumps({"error" : fail_response})
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(final_message.encode("utf-8"))

        if re.search("/isalive", self.path):
            # This accepts the health check request from the frontend and returns 200 status code if the service is up
            # print("Inside isalive")
            final_message = json.dumps({"instance_id": INSTANCE_ID, "status": "OK"} )
            self.send_response(200)
            self.send_header("Content-type", 'application/json')
            self.end_headers()
            self.wfile.write(final_message.encode("utf-8"))
            return

    def do_POST(self):
        global LEADER_ID
        global ALL_ORDER_NODES
        global LEADER_HOST
        global LEADER_PORT
        global memory_data

        if re.search("/trade", self.path):
            # reads the request which is in form of a json object
            length = int(self.headers.get('Content-Length', 0))
            request_body = self.rfile.read(length)
            request_data = json.loads(request_body)

            # the components are extracted from the request
            name = request_data["name"]
            quantity = request_data["quantity"]
            type = request_data["type"]

            # the url used to connect with the catalog service
            catalog_url = "http://" + self.catalog_name + ":" + str(self.catalog_port) + "/trade"


            # the details are sent as a JSON object to the catalog service
            details = {
                "name" : name,
                "quantity" : quantity,
                "type" : type,
            }

            headers = {
                'Content-Length' : str(length),
                'Content-type' : "application/json",
            }

            # the response received back
            response = requests.post(catalog_url, json = details, headers = headers)

            # if the response has the status 404 then the error response is sent to the front-end
            if response.status_code == 404:
                invalid_name = {
                    "code" : 404,
                    "message" : "stock not found",
                }
                print(f"Error: {invalid_name}")
                invalid_response = json.dumps({"error" : invalid_name})

                self.send_response(404)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(invalid_response.encode("utf-8"))
                return

            # if the response has the status 400 which means the amount of quantity to be bought is less than what is available
            if response.status_code == 400:
                insufficient_quantity = {
                    "code" : 400,
                    "message" : "insufficient quantity",
                }
                print(f"Error: {insufficient_quantity}")
                answer = json.dumps({"error" : insufficient_quantity})

                self.send_response(400)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(answer.encode("utf-8"))
                return

            # if the response has status 200 it means that the trade was successful and the transaction number of the trade is sent back
            if response.status_code == 200:
                print(f"Calculating txn number")
                # First calculate the transaction num to be assigned
                transaction_number = self.calculate_txn_number()
                new_detail = {
                    'Transaction number' : transaction_number,
                    'name' : name,
                    'order type' : type,
                    'quantity' : quantity
                }

                print(f"Before assigning previous txn num {memory_data}")
                previous_transaction_num = None
                if memory_data:
                    # If there were any previous transactions in the memory data, then get the number of the latest one in it
                    previous_transaction_num = get_last_txn_number()
                append_to_memory_data(new_detail)

                write_mem_data_to_file(self.order_log)

                right_answer = {
                    'transaction number' : transaction_number,
                }
                print(f"Broadcasting successful trade")
                # Once a successful trade request is made, call the broadcast_successful_trade function to maintain data consistency
                self.broadcast_successful_trade(new_detail, previous_transaction_num)

                final = json.dumps({"data" : right_answer})
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(final.encode("utf-8"))

                return

        if re.search("/notify", self.path):
            # API that notifies the current instance of the leader and details all other nodes in the network
            # print("Inside notify")
            # reads the request which is in form of a json object
            length = int(self.headers.get('Content-Length', 0))
            request_body = self.rfile.read(length)
            request_data = json.loads(request_body)
            LEADER_ID = request_data["leader"]
            print(f"The leader is: {LEADER_ID}")
            ALL_ORDER_NODES = request_data["all_order_nodes"]
            # print(f"ALL_ORDER_NODES: {ALL_ORDER_NODES}")
            for k,v in ALL_ORDER_NODES.items():
                if v["instance_id"] == LEADER_ID:
                    LEADER_HOST = v["host"]
                    LEADER_PORT = v["port"]
            self.send_response(200)
            self.send_header("Content-type", 'application/text')
            self.send_header("Content-length", len(b"OK"))
            self.end_headers()
            self.wfile.write(b"OK")
            return

        if re.search("/updateOrderLog", self.path):
            # API that is called by the leader to maintain data consistency among all the follower nodes 
            # and is executed only if the receiver is a follower node. 
            # It updates the order logs of all the follower nodes with the latest order information
            if INSTANCE_ID != LEADER_ID:
                length = int(self.headers.get('Content-Length'))
                request_body = self.rfile.read(length)
                request_data = json.loads(request_body)
                # print(f"For updating order log, data received: {request_data}")
                successful_order_data = request_data["successful_order_data"]
                leader_previous_txn_num = request_data["previous_txn_num"]
                current_leader_details = request_data["current_leader_details"]
                print("leader_previous_txn_num", leader_previous_txn_num)
                # print("Memory data", memory_data)
                print("Last txn no of current instance", get_last_txn_number())
                if leader_previous_txn_num: # if there's no prev txn number then this is the first txn
                    # Checks if the data in the follower node log files is in sync with the leader node logs
                    if memory_data and get_last_txn_number() == leader_previous_txn_num:
                        append_to_memory_data(successful_order_data)
                        write_mem_data_to_file(self.order_log)
                        update_txn_number(successful_order_data['Transaction number'])
                    else:
                        # If the last transaction number of the follower node and the leader node's previous transaction number 
                        # do not match then it means the follower node had crashed and now its back alive and looking to sync its data.
                        print("Calling syncdata", INSTANCE_ID)
                        LEADER_HOST = current_leader_details["host"]
                        LEADER_PORT = current_leader_details["port"]
                        LEADER_ID = current_leader_details["instance_id"]
                        order_url = "http://" + LEADER_HOST + ":" + str(LEADER_PORT) + "/syncOrderData"
                        last_txn = {"last_txn": memory_data[-1]['Transaction number']}
                        response_body_bytes = json.dumps(last_txn).encode(encoding='utf_8')
                        response_content_length = len(response_body_bytes)
                        headers = {
                        'Content-Length': str(response_content_length),
                        'Content-type': "application/json",
                        }
                        resp = requests.post(order_url, json=last_txn, headers=headers)
                        all_missed_txns = resp.json()["all_missed_txns"]
                        # now append all the transactions that the follower node missed, into its order logs
                        extend_memory_data(all_missed_txns)
                        write_mem_data_to_file(self.order_log)
                        update_txn_number(all_missed_txns[-1]['Transaction number'])
                else:
                    # For initial transaction
                    append_to_memory_data(successful_order_data)
                    write_mem_data_to_file(self.order_log)
                    update_txn_number(successful_order_data['Transaction number'])
                self.send_response(200)
                self.send_header("Content-type", 'application/text')
                self.send_header("Content-length", len(b"OK"))
                self.end_headers()
                self.wfile.write(b"OK")
            return

        if re.search("/syncOrderData", self.path):
            # API to make sure that when a crashed replica is back online, it can synchronize with the other replicas 
            # to retrieve the order information that it has missed during the offline time.
            if INSTANCE_ID == LEADER_ID:
                follower_last_txn_index = None
                print("Inside syncdata")
                length = int(self.headers.get('Content-Length', 0))
                request_body = self.rfile.read(length)
                request_data = json.loads(request_body)
                # retreive the last transaction that was updated in the follower log file before it crashed
                follower_last_txn = request_data["last_txn"]
                print("Last txn", follower_last_txn)
                follower_last_txn_index = search_txn_mem_data(follower_last_txn)
                if not follower_last_txn_index:
                    print(f"Txn number {follower_last_txn} was not found, inconsistent state detected, stopping....")
                    exit()
                print("follower_last_txn_index", follower_last_txn_index)
                # retreive all the transactions from the leader node's log file memory data
                all_missed_transactions = memory_data[follower_last_txn_index+1:]
                print("All missed", all_missed_transactions)
                missed = {"all_missed_txns" : all_missed_transactions}
                resp = json.dumps(missed).encode(encoding='utf_8')
                response_content_length = len(resp)
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.send_header("Content-length", response_content_length)
                self.end_headers()
                self.wfile.write(resp)
            return


    def broadcast_successful_trade(self, new_detail, previous_transaction_num):
        # This sends the successful trade details along with the leader's previous transaction 
        # number and the current order details to all the other nodes in the network
        if INSTANCE_ID == LEADER_ID:
            for node_num in list(ALL_ORDER_NODES.keys()):
                node_host = ALL_ORDER_NODES[node_num]["host"]
                node_port = ALL_ORDER_NODES[node_num]["port"]
                node_id = ALL_ORDER_NODES[node_num]["instance_id"]
                if node_id != INSTANCE_ID:
                    order_url = "http://" + node_host + ":" + str(node_port) + "/updateOrderLog"
                    content_length = int(self.headers.get('Content-Length'))
                    headers = {
                    'Content-Length': str(content_length),
                    'Content-type': "application/json",
                    }
                    try:
                        success_data = {
                            "successful_order_data": new_detail, 
                            "previous_txn_num": previous_transaction_num,
                            "current_leader_details": {
                                "host": LEADER_HOST,
                                "port": LEADER_PORT,
                                "instance_id": LEADER_ID
                            }
                        }
                        print(f"Broadcasting trade to {order_url}")
                        requests.post(order_url, json=success_data, headers=headers)
                    except:
                        print(f"Failed to send successful order data to {node_id}")
                        continue
        return

# just like in catalog service, thread-per-request model is used for the order service
class Order_with_Threads(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True

if __name__ == "__main__":

    # Accept instance id via command line argument
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--instanceid", help="Unique ID of each replica", required=True)
    parser.add_argument("-n", "--host", help="Hostname of each replica", required=True)
    parser.add_argument("-p", "--port", help="Port no of each replica", required=True)
    args = parser.parse_args()
    INSTANCE_ID = str(args.instanceid)
    print(f"Instance ID: {INSTANCE_ID}")
    hostName = str(args.host)
    PORT = int(args.port)
    order_log_2 = os.path.join(os.getcwd(), "purchase_app", "resources", f"order_log_{INSTANCE_ID}.csv")
    try:
        # create file
        with open(order_log_2, 'x') as fp:
            print(f"File created at: {order_log_2}")
    except:
        # print('File already exists')
        pass
    # the data from the order log is read into the memory_data
    with open(order_log_2, 'r', encoding = 'utf-8-sig') as file:
        read = csv.DictReader(file)
        for data in read:
            append_to_memory_data(data)
    # if the file already has data in it, get the last transaction number
    get_last = get_last_txn_number()
    if get_last:
        update_txn_number(get_last)
    server = Order_with_Threads((hostName, PORT), Order)
    print("Order service started http://%s:%s" % (hostName, PORT))

    # the server stops only at a keyboard interruption
    try:
        server.serve_forever()

    except KeyboardInterrupt:
        server.shutdown()