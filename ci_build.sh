#!/bin/bash -l
set -e
cd $FOLDER_NAME && bundle install --deployment && bundle exec rake spec:ci --trace


