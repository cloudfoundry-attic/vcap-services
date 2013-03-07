#!/bin/bash 
set -e
sudo mkdir -p /var/vcap/packages
sudo mkdir -p /var/vcap/store
sudo chown -R $USER /var/vcap
./download_binaries_from_s3.rb
tar xf common.tar.gz -C /
tar xf packages.tar.gz -C /
rm -f common.tar.gz packages.tar.gz
cd $FOLDER_NAME && bundle install --local && bundle exec rake spec --trace
