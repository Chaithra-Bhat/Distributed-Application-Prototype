# Distributed-Application-Prototype
Prototype of a fault-tolerant and reliable distributed application

This project aims to demonstrate a client-server application that incorporates caching and replication and exhibits fault tolerance and high availability. Additionally, it has also been deployed on AWS cloud. Various aspects are given as follows:
* All the environment variables are set in the `env_setup.sh` shell script.
* There are three micro-services, namely `product_app.py`, `purchase_app.py` and `frontend_app.py`, each of which are run simultaneously on different terminals.
* The client code is used to run multiple clients to measure performance and latency.
