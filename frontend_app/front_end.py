from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import re
import requests
import json
import os

# specifying the host and port number that front-end service will run on
#hostName = "localhost"
hostName = os.getenv("FRONTEND_HOSTNAME")
#PORT = 8516
PORT = int(os.getenv("FRONTEND_PORT"))
# print(f"From env, hostname:{hostName} and port: {PORT}")

# checking if caching is enabled
enable_cache = os.getenv("ENABLE_CACHE", "True") == "True"

# if caching is enabled
if enable_cache:
    # in memory cache as a dictionary
    cache = {}

# if caching is off   
else:
    # disable cache
    cache = None

order_service_instances = {}

LEADER_ID = None
LEADER_NUM = None
tradeHostName = None
tradePort = None

class FrontEnd(BaseHTTPRequestHandler):
    # http protocol version that supports persistent client connections
    protocol_version = 'HTTP/1.1'
    
    def __init__(self, *args, **kwargs):
        # declaring leader_id and its respective number in the list of order nodes as global
        global LEADER_ID
        global LEADER_NUM
        # specifying the host and port number of catalog service
        self.lookupHostName = os.getenv("CATALOG_HOSTNAME")
        self.lookupPort = int(os.getenv("CATALOG_PORT"))
        # print(f"From env, CATALOG hostname:{self.lookupHostName} and port: {self.lookupPort}")

        # get details of ll the order service instances in the network that is declared previously
        self.total_order_instances = int(os.getenv("TOTAL_ORDER_INSTANCES"))
        for i in range(1, self.total_order_instances+1):
            order_service_instances[i] = {
                "host": os.getenv(f"ORDER_{str(i)}_HOSTNAME"),
                "port": int(os.getenv(f"ORDER_{str(i)}_PORT")),
                "instance_id": os.getenv(f"ORDER_{str(i)}_INSTANCE_ID")
            }
        
        if not LEADER_ID:
            # perform leader selection
            LEADER_ID, LEADER_NUM = self.get_order_leader()
            self.assign_order_leader()
            self.announce_leader()

        # invoking the base class constructor
        super().__init__(*args, **kwargs)
    
    def get_order_leader(self):
        # print("Inside get leader")
        # iterate through the list in descending order
        for instance_num in reversed(order_service_instances.keys()):
            instance_host = order_service_instances[instance_num]["host"]
            instance_port = order_service_instances[instance_num]["port"]
            instance_id = order_service_instances[instance_num]["instance_id"]
            try:
                # creating the url to invoke the REST API of the order service that checks if the order service is alive or crashed
                order_url = "http://" + instance_host + ":" + str(instance_port) + "/isalive"
                print("Sending to order url", order_url)
                health_check = requests.get(order_url) # sending a GET request
                
                # if the order is alive, then its assigned as the leader
                if(health_check.status_code == 200):
                    healthy_order_service = health_check.json()
                    print(f"Leader: {healthy_order_service}")
                    
                return instance_id, instance_num
            except:
                print(f"Order service {order_service_instances[instance_num]} is not alive")
                continue
        return
    
    def assign_order_leader(self):
        # this function assigns the leader host and port to its respective global variables
        global tradeHostName
        global tradePort
        # print("Assign leader")
        if LEADER_ID:
            print("Host and port of leader order service", order_service_instances[LEADER_NUM])
            # specifying the host and port number of the leader order service
            tradeHostName = order_service_instances[LEADER_NUM]["host"]
            tradePort = order_service_instances[LEADER_NUM]["port"]
        else:
            # if the leader id is not assigned when this function is called, it means none of the services are alive
            print("All the order service instances are down")
            exit()
    
    def announce_leader(self):
        # print("Inside announce leader")
        for instance_num in reversed(order_service_instances.keys()):
            instance_host = order_service_instances[instance_num]["host"]
            instance_port = order_service_instances[instance_num]["port"]

            try:
                # creating the url to invoke the REST API of the order service that notifies all nodes 
                # with details of the nodes in the network and the leader id
                order_url = "http://" + instance_host + ":" + str(instance_port) + "/notify"
                response_body = {"leader": LEADER_ID, "all_order_nodes": order_service_instances}
                response_body_bytes = json.dumps(response_body).encode(encoding='utf_8')
                response_content_length = len(response_body_bytes)
                headers = {
                'Content-Length': str(response_content_length),
                'Content-type': "application/json",
                }
                requests.post(order_url, json=response_body, headers=headers)
            except:
                print(f"Failed to notify {instance_host}/{instance_port}")
                continue

    # function to handle all GET requests
    def do_GET(self):
        # verifying whether the REST API path for a GET request is correct
        if re.search("/stocks/.*", self.path):
            stock_data = self.lookup_stock_details()
            self.send_api_response(stock_data)
            return
        elif re.search("/test", self.path):
            self.send_response(200)
            self.send_header("Content-type", 'application/text')
            self.send_header("Content-length", len(b"OK"))
            self.end_headers()
            self.wfile.write(b"OK")
            return
        
        # if the user decides to query an order
        elif re.search("/orders/.*", self.path):
            query_data = self.query_order_number()
            self.send_api_response(query_data)
            return
    
    # function to handle all POST requests
    def do_POST(self):
        global LEADER_NUM
        global LEADER_ID
        
        # verifying whether the REST API path for a POST request is correct
        if re.search("/orders", self.path):
            stock_trade = self.handle_stock_trade()
            self.send_api_response(stock_trade)
            return
        
        # gets the invalidation request and removes the item from the cache
        elif self.path == '/invalidate_cache':
            length = int(self.headers.get('Content-length'))
            body = self.rfile.read(length)
            req = json.loads(body.decode('utf-8'))
            name = req["name"]
            
            # the removal part
            if name in cache:
                del cache[name]

            response = {'message' : 'stock removed from cache'}
            answer = json.dumps(response).encode('utf-8')
            response_content_length = len(answer)    
            # a successful response is sent back
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-length", response_content_length)
            self.end_headers()
            self.wfile.write(answer)
            return
                      
    
    # function to send responses from the respective GET/POST requests back to the caller
    def send_api_response(self, api_data):
        response_code = api_data.status_code
        response_body = api_data.json()
        response_body_bytes = json.dumps(response_body).encode(encoding='utf_8')
        response_content_type = api_data.headers['Content-type']
        response_content_length = len(response_body_bytes) # to enable the implementation of a thread-per-session model
        self.send_response(response_code)
        self.send_header("Content-type", response_content_type)
        self.send_header("Content-length", response_content_length)
        self.end_headers()
        # write the body of the response back to the client using an output stream
        self.wfile.write(response_body_bytes)

    # function to implement the API that forwards requests to the catalog service to look up the details of a stock
    def lookup_stock_details(self):
        stock_name = self.path.split("/stocks/")[1] # get stock name from the url
        
        # checks if its already in cache and returns if it is
        if stock_name in cache:
            answer = {
                "name" : stock_name,
                "price" : cache[stock_name][0],
                "quantity" : cache[stock_name][1],
            }
            
            print(cache)
            response = json.dumps({"data" : answer})
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(response.encode("utf-8"))
            return
        
        elif cache is not None:
            # creating the url to invoke the REST API of the catalog service
            catalog_url = "http://" + self.lookupHostName + ":" + str(self.lookupPort) + "/lookup/" + stock_name
            stock_data = requests.get(catalog_url) # sending a GET request
            
            # the details are added to the cache with the stock name as key and a list of price and quantity as value 
            if(stock_data.status_code == 200):
                answer = stock_data.json()
                name = answer["data"]["name"]
                cache[name] = []
                price = answer["data"]["price"]
                quantity = answer["data"]["quantity"]
                cache[name].append(price)
                cache[name].append(quantity)
                
            print(cache)
            return stock_data
        
    
    # function to implement the API that forwards requests to the order service to query a particular order's details
    def query_order_number(self):
        global LEADER_NUM
        global LEADER_ID
        order_no = self.path.split("/orders/")[1] # get the order number from the url

        while True:
            try:
                # creating the url to invoke the query API
                order_url = "http://" + tradeHostName + ":" + str(tradePort) + "/query/" + order_no
                order_data = requests.get(order_url) # sending a GET request
                return order_data
            except:
                # If the leader service does not reply back, it means the leader has crashed and leader re-election is triggered
                print("Leader is unresponsive")
                LEADER_ID, LEADER_NUM = self.get_order_leader()
                self.assign_order_leader() 
                self.announce_leader()

    # function to implement the API that forwards requests to the order service to place an order for a certain stock
    def handle_stock_trade(self):
        global LEADER_NUM
        global LEADER_ID
        # reading the request body (json object) of the POST API
        content_length = int(self.headers.get('Content-Length'))
        # read the body of the request from the client using an input stream
        post_body = self.rfile.read(content_length)
        request = json.loads(post_body.decode('utf-8'))
        name = request["name"]
        trade = request["type"]
        quantity = request["quantity"]
        trade_details = {
            "name": name,
            "type": trade,
            "quantity": quantity,
        }
        headers = {
            'Content-Length': str(content_length),
            'Content-type': "application/json",
        }
        while True:
            try:
                # creating the url to invoke the REST API of the order service
                order_url = "http://" + tradeHostName + ":" + str(tradePort) + "/trade"
                # order_url = "https://df010cd7-2553-430e-972b-493f1a57b268.mock.pstmn.io" + "/trade"
                order_data = requests.post(order_url, json=trade_details, headers=headers) # sending a POST request
                return order_data
            except:
                # If the leader service does not reply back, it means the leader has crashed and leader re-election is triggered
                print("Leader is unresponsive")
                LEADER_ID, LEADER_NUM = self.get_order_leader()
                self.assign_order_leader()
                self.announce_leader()

class FrontendThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Implements multithreading into the HTTP server enabling it to spawn a new thread for each session"""

if __name__ == "__main__":
    # Instantiating the FrontendThreadedHTTPServer class that allows multithreading in an HTTP server
    frontendServer = FrontendThreadedHTTPServer((hostName, PORT), FrontEnd)
    print("Front end service started http://%s:%s" % (hostName, PORT))

    try:
        # starts the http server to handle requests
        frontendServer.serve_forever()
    except KeyboardInterrupt:
        pass
    
    # stop the server after execution is completed or due to keyboard interrupt
    frontendServer.server_close()
    print("Server has stopped.")