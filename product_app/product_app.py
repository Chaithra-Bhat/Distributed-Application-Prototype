import csv
from http.server import BaseHTTPRequestHandler, HTTPServer
import socketserver
import json
import threading
import os
import requests

#hostName = "localhost"
hostName = os.getenv("CATALOG_HOSTNAME")

# the service uses the port 9000
#PORT = 9020

PORT = int(os.getenv("CATALOG_PORT"))
#PORT = 8515

front_host = os.getenv("FRONTEND_HOSTNAME")
#front_host = "localhost"

front_port = int(os.getenv("FRONTEND_PORT"))
#front_port = 8516

enable_cache = os.getenv("ENABLE_CACHE", "True") == "True"

disk_file = os.path.join(os.getcwd(), "product_app", "resources", "catalog.csv")
class Service(BaseHTTPRequestHandler):
    # RLock is used for synchronization, multiple readers can acquire at the same time but for writing only one can.
    lock = threading.RLock()
    
    #memory_data is the in memory storage for catalog details obtained from the csv file
    memory_data = []   
    with open(disk_file, 'r', encoding = 'utf-8-sig') as file:
        read = csv.DictReader(file)
        
        # the data from read is appended to the memory_data initialized above
        for data in read:
            memory_data.append(data)
               
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

                
    # This function is used to write the updated data to the csv file as mentioned in the description
    def save_to_file(self):
        with self.lock:           
            # the data in the csv is overwritten with the latest changes in the memory_data
            with open(disk_file, 'w', newline = '') as file:
                write = csv.DictWriter(file, fieldnames = ['name', 'price', 'quantity'])
                write.writeheader()
            
                for stock in self.memory_data:
                    write.writerow(stock)
                
    # this function is for the LookUp requests from the front-end service
    def do_GET(self):
        
        # verifies that the path of the front-end service is correct
        if self.path.startswith("/lookup/"):
            stock_name = self.path.split("/lookup/")[1]
            
            # a read lock for every read request
            with self.lock:            
                for data in self.memory_data:
                                       
                    # checks if the name in the request is valid and returns the response if it is 
                    if data["name"] == stock_name:
                        request_response = {
                            "name" : data["name"],
                            "price" : data["price"],
                            "quantity" : data["quantity"]
                        }
                        
                        # successful response sent back to front-end service
                        
                        final_response = json.dumps({"data" : request_response})
                        self.send_response(200)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(final_response.encode("utf-8"))
                        return
                   
            # error message for invalid stock name 
            fail_response = {
                "code" : 404,
                "message" : "stock not found"
            }
            
            # error message sent to the front-end service
            final_message = json.dumps({"error" : fail_response})
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(final_message.encode("utf-8"))
             
    # this function is for the trade requests from the order service
    def do_POST(self):
        
        # checks the path to the order service
        if self.path.startswith("/trade"):
            
            # as the request is a JSON object, the data is read from it
            length = int(self.headers.get('Content-Length'))
            post_body = self.rfile.read(length)
            request = json.loads(post_body.decode('utf-8'))
            
            # the 3 components of the trade request are mentioned
            name = request["name"]
            trade = request["type"]
            quantity = request["quantity"]
            
            # a write lock that only happens with a single writer
            with self.lock:                      
                for data in self.memory_data:
                    
                    # only works if the stock name is valid
                    if data["name"] == name:
                        current_volume = int(data["quantity"])
                    
                        # if the request is to sell, then the quantity is incremented and the response is sent along with with an empty json object
                        if trade == "sell":
                            new_volume = current_volume + quantity
                            data["quantity"] = str(new_volume)
                            self.save_to_file()
                            right_response = {
                                "message" : "successfully sold",
                            }
                            
                            if enable_cache:
                                # invalidation request sent to front end
                                requests.post(f"http://{front_host}:{front_port}/invalidate_cache", json={"name": name})
                             
                            answer = json.dumps({"data" : right_response}).encode("utf-8")
                            response_content_length = len(answer)
                            self.send_response(200)
                            self.send_header("Content-type", "application/json")
                            self.send_header("Content-length", response_content_length)
                            self.end_headers()
                            self.wfile.write(answer)
                            return
                        
                        # This is for a buy trade
                        elif trade == "buy":
                            
                            # if the current quantity is lesser than the quantity requested to be bought. An error response is sent 
                            if current_volume < quantity:
                                incorrect_response = {
                                    "code" : 400,
                                    "message" : "Insufficient quantity",
                                }
                                                           
                                final = json.dumps({"error" : incorrect_response})
                                self.send_response(400)
                                self.send_header("Content-type", "application/json")
                                self.end_headers()
                                self.wfile.write(final.encode("utf-8"))
                                return
                        
                           # otherwise the trade is successful and the quantity sold is decremented and the right response is sent. An empty json object is sent
                            else:
                                new_volume = current_volume - quantity
                                data["quantity"] = str(new_volume)
                                self.save_to_file()
                                right_response = {
                                    "message" : "successfully bought",
                                }
                                
                                if enable_cache:
                                    # invalidation request sent to front end
                                    requests.post(f"http://{front_host}:{front_port}/invalidate_cache", json={"name": name})
                                    
                                answer = json.dumps({"data" : right_response})
                                self.send_response(200)
                                self.send_header("Content-type", "application/json")
                                self.end_headers()
                                self.wfile.write(answer.encode("utf-8"))
                                return
                                          
            # this is sent if an incorrect stock name is specified
            invalid_response = {
                "code" : 404,
                "message" : "stock not found",
            }
            
            result = json.dumps({"error" : invalid_response})
            
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(result.encode("utf-8"))
            return
                    
# this class is created for employing threads to handle multiple requests. ThreadingMinIn creates a thread for every request
class Server_with_threads(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True

if __name__ == "__main__":
    
    # the server is started and the message is printed to acknowledge
    server = Server_with_threads((hostName, PORT), Service)
    print("Catalog service started http://%s:%s" % (hostName, PORT))
    
    try:
        server.serve_forever()
        
    # the server is stopped by pressing any key
    except KeyboardInterrupt:
        server.shutdown()