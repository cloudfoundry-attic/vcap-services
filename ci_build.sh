#!/bin/bash -l
set -e
sudo mkdir -p /var/vcap
sudo chown travis:travis /var/vcap
cd $FOLDER_NAME && bundle install --deployment && bundle exec rake spec --trace
