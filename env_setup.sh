#!/bin/sh

# Set all the required environment variables
export CATALOG_HOSTNAME=localhost
export CATALOG_PORT=8515
export FRONTEND_HOSTNAME=localhost
export FRONTEND_PORT=8516

unset ENABLE_CACHE

# Create 3 (or more) order service env variables
export TOTAL_ORDER_INSTANCES=3

export ORDER_1_HOSTNAME=localhost
export ORDER_1_PORT=8661
export ORDER_1_INSTANCE_ID=id1

export ORDER_2_HOSTNAME=localhost
export ORDER_2_PORT=8662
export ORDER_2_INSTANCE_ID=id2

export ORDER_3_HOSTNAME=localhost
export ORDER_3_PORT=8663
export ORDER_3_INSTANCE_ID=id3