#!/bin/bash

docker exec --workdir /opt/kafka/bin controller sh -c \
  './kafka-topics.sh --create --bootstrap-server broker1:19092,broker2:19092,broker3:19092 --topic work.firmino.orders --partitions 3 --replication-factor 1'