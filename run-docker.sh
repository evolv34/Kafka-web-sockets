#!/bin/bash

#docker rm -f $(docker ps -aq)
docker login -u egenprad -p 123456

docker-compose -f docker-compose-kafka.yml up