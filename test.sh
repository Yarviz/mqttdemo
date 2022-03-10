#!/bin/bash

gnome-terminal --geometry 80x25+150+10 -- python3 src/server.py
gnome-terminal --geometry 80x25+900+10 -- python3 src/client.py
gnome-terminal --geometry 80x25+150+520 -- python3 src/client.py
gnome-terminal --geometry 80x25+900+520 -- python3 src/client.py
