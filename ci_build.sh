#!/bin/bash -l
set -e
cd $FOLDER_NAME && bundle exec rake spec:ci

