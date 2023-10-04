import random
import requests
import os
import time
import pickle
import argparse
from statistics import mean

#specifying the host and port number of the front-end service that the client needs to access
frontendHostName = os.getenv("FRONTEND_HOSTNAME")
frontendPORT = int(os.getenv("FRONTEND_PORT"))
probabilities = [0.2, 0.4, 0.6, 0.8] # probability of making another order after a successful lookup
successful_trade_requests = []

# lists to calculate all the requests latencies
lookup_latency = []
trade_buy_latency = []
trade_sell_latency = []
query_latency = []

def send_requests(client_fname):
    found_stock = False
    stock_names = ["GameStart", "FishCo", "BoarCo", "MenhirCo", "Google", "Apple", "Meta",
                   "Amazon", "Netflix", "Microsoft", "invalid"]
    trade_quantity = random.randrange(0, 10)
    client_latency_time = {}

    # creating the urls to invoke the GET and POST REST APIs of the front-end service
    lookup_url = frontendHostName + ":" + str(frontendPORT) + "/stocks/"
    trade_url = frontendHostName + ":" + str(frontendPORT) + "/orders"
    query_url = frontendHostName + ":" + str(frontendPORT) + "/orders/"

    # randomly looks up a stock in the list stock_names
    rand_stock_name = random.choice(stock_names)

    # opens an HTTP connection with the front-end service to maintain a session
    session = requests.Session()

    for p in probabilities:
        lookup_latency = []
        trade_buy_latency = []
        trade_sell_latency = []
        query_latency = []
        # If the probability is greater than the randomly generated number, make a sequence of requests based on the probability p
        while random.uniform(0, 1) <= p:
            # invoking GET API to look up the details of a stock
            # Also calculating the lookup latency time
            lookup_start_time = time.perf_counter()
            lookup_response = session.get("http://" + lookup_url + rand_stock_name)
            lookup_end_time = time.perf_counter()
            lookup_latency.append(lookup_end_time - lookup_start_time)

            # If the front-end service returns an error code, display the error and break flow of execution
            if lookup_response.status_code != 200:
                print(lookup_response.json())
                break
            else:
                found_stock = True
                # convert response to a json format to perform further processes
                json_lookup_resp = lookup_response.json()
                quantity = json_lookup_resp['data']['quantity']

                # If the quantity is greater than zero, client may make another order request based on the probability p
                if int(quantity) > 0:
                    # First we'll buy stocks and then sell those stocks
                    # Check if there are enough stocks to buy
                    if int(quantity) > trade_quantity:
                        # create a json data object
                        trade_details = {
                            "name": rand_stock_name,
                            "type": "buy",
                            "quantity": trade_quantity,
                        }
                        # invoke the POST API of the front-end service to buy stocks
                        # Also calculating the buy trade latency time
                        buy_start_time = time.perf_counter()
                        trade_response = session.post("http://" + trade_url, json=trade_details)
                        buy_end_time = time.perf_counter()
                        trade_buy_latency.append(buy_end_time - buy_start_time)
                        buy_resp = trade_response.json()['data']
                        if trade_response.status_code == 200:
                            print("Bought stock",buy_resp)
                            successful_trade_requests.append({
                                'txn_no': buy_resp["transaction number"],
                                "name": rand_stock_name,
                                "type": "buy",
                                "quantity": trade_quantity,
                            })
                        else:
                            print("Buy trade failed")
                    else:
                        trade_details = {
                            "name": rand_stock_name,
                            "type": "buy",
                            "quantity": trade_quantity,
                        }
                        # invoke the POST API of the front-end service to buy stocks
                        buy_start_time = time.perf_counter()
                        trade_response = session.post("http://" + trade_url, json=trade_details)
                        buy_end_time = time.perf_counter()
                        trade_buy_latency.append(buy_end_time - buy_start_time)
                        print("Insufficient quantity", trade_response.json())

                    trade_details = {
                        "name": rand_stock_name,
                        "type": "sell",
                        "quantity": trade_quantity,
                    }
                    # invoke the POST API of the front-end service to sell stocks
                    sell_start_time = time.perf_counter()
                    trade_response = session.post("http://" + trade_url, json=trade_details)
                    sell_end_time = time.perf_counter()
                    trade_sell_latency.append(sell_end_time - sell_start_time)
                    sell_resp = trade_response.json()['data']
                    if trade_response.status_code == 200:
                        print("Sold stock",sell_resp)
                        successful_trade_requests.append({
                            'txn_no': sell_resp["transaction number"],
                            "name": rand_stock_name,
                            "type": "sell",
                            "quantity": trade_quantity,
                        })
                    else:
                        print("Sell trade failed")
        if found_stock:
            # Retrieves the order information of each order that was made using the order query request, 
            # and checks whether the server reply matches the locally stored order information.
            for every_txn in successful_trade_requests:
                txn_no = every_txn['txn_no']
                query_start_time = time.perf_counter()
                query_response = session.get("http://" + query_url + str(txn_no))
                query_end_time = time.perf_counter()
                query_latency.append(query_end_time - query_start_time)
                if query_response.status_code == 200:
                    query_body = query_response.json()['data']
                    if query_body["number"] == txn_no and query_body['name'] == every_txn['name'] and query_body['type'] == every_txn['type'] and query_body['quantity'] == every_txn['quantity']:
                        print(f"The server reply matches the locally stored order information for txn_no {txn_no}")
                    else:
                        print(f"Server reply does not match for txn_no Every txn: {every_txn} and query data: {query_body}")
                else:
                    print(f"Failed to retreive txn no {txn_no}")
        else:
            print("Stock not found")
        # calculating the average latency of a client
        if len(lookup_latency):
            client_latency_time[f"lookup_latency_{str(p)}"] = mean(lookup_latency)
        else:
            client_latency_time[f"lookup_latency_{str(p)}"] = 0
        
        if len(trade_buy_latency):
            client_latency_time[f"trade_buy_latency_{str(p)}"] = mean(trade_buy_latency)
        else:
            client_latency_time[f"trade_buy_latency_{str(p)}"] = 0
        
        if len(trade_sell_latency):
            client_latency_time[f"trade_sell_latency_{str(p)}"] = mean(trade_sell_latency)
        else:
            client_latency_time[f"trade_sell_latency_{str(p)}"] = 0

        if len(query_latency):
            client_latency_time[f"query_latency_{str(p)}"] = mean(query_latency)
        else:
            client_latency_time[f"query_latency_{str(p)}"] = 0

    filename = client_fname
    # print(client_fname, client_latency_time)
    with open(filename, 'wb') as f: # dumping results in a pickle file in order to measure latency of multiple clients
        pickle.dump(client_latency_time, f)
    # closes the HTTP connection with the front-end service to end the session
    session.close()


if __name__ == "__main__":
   # Accept the pickle client file name via command line argument
   parser = argparse.ArgumentParser()
   parser.add_argument("-c","--clientfname", help="Specify client file name", default="latency.pkl")
   args = parser.parse_args()
   send_requests(args.clientfname)