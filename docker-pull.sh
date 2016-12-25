#!/bin/bash

while true; do
		
		docker pull chanhub/kafka-realtime
		docker rmi -f $(docker images -a | grep "chanhub/kafka-realtime" | awk '{print $3}')
	done