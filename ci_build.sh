#!/bin/bash 
set -e
sudo mkdir -p /var/vcap
sudo chown $USER /var/vcap
cd $FOLDER_NAME && bundle install --deployment && bundle exec rake spec --trace
