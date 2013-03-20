#!/bin/bash

kill_process(){
  local name=$1
  ps aux | grep $name | grep -v grep | awk '{print $2}' | xargs kill
}
# stop nats server
kill_process nats-server

# stop service redis
kill_process redis-server

# stop cloud controller
if [[ -f /tmp/cloud_controller.pid ]]; then
  cat /tmp/cloud_controller.pid | xargs kill
fi

# stop service gateways and nodes
kill_process mysql_gateway
kill_process mysql_node

# delete old database
rm -f /tmp/cloud_controller.db

