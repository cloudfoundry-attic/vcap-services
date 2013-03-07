#!/bin/bash 
set -e
sudo mkdir -p /var/vcap/packages
sudo mkdir -p /var/vcap/store
sudo chown -R $USER /var/vcap

START_DIR=`pwd`
(
  git clone --recursive --depth=100 --quiet --branch=master git://github.com/cloudfoundry/warden.git warden
  cd warden

  # Ignore this project BUNDLE_GEMFILE
  unset BUNDLE_GEMFILE

  # Close stdin
  exec 0>&-

  # Remove remnants of apparmor (specific to Travis VM)
  sudo dpkg --purge apparmor

  # Install dependencies
  sudo apt-get -y install debootstrap quota

  cd warden
  bundle install --deployment
  rvmsudo bundle exec rake setup[config/linux.yml]
  rvmsudo bundle exec rake warden:start[config/linux.yml] >>/tmp/warden.stdout.log 2>>/tmp/warden.stderr.log &
)

# Wait for warden to come up
while [ ! -e /tmp/warden.sock ]
do
  sleep 1
done

echo "/tmp/warden.sock exists, let's run the specs"

cd $START_DIR/$FOLDER_NAME && ([[ ! -e ci_prepare.sh ]] || ./ci_prepare.sh) && rvmsudo bundle install --local && rvmsudo bundle exec rake spec --trace
