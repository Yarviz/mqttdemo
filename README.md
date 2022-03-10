# MQTT Demo

Simple example program which uses subscribe/publish protocol to send messages between clients.

## Running

start server

    python3 src/server.py [--host HOST] [--port PORT]

start client

    python3 src/client.py [--host HOST] [--port PORT]

start server and 3 clients (with deafult settings)

    ./test.sh