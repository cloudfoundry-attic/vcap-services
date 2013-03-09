#!/bin/bash 
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
  rvmsudo bundle exec rake setup:bin
  # Get waren rootfs
  $START_DIR/download_binaries_from_s3.rb warden_rootfs.tar.gz
  sudo mkdir -p /tmp/warden/rootfs
  sudo tar zxf warden_rootfs.tar.gz -C /tmp/warden/rootfs 
  rvmsudo bundle exec rake warden:start[config/linux.yml] >>/tmp/warden.stdout.log 2>>/tmp/warden.stderr.log &
)
cd $START_DIR

# Wait for warden to come up
sleeps=15
while [ ! -e /tmp/warden.sock ] && [ $sleeps -gt 0 ]
do
  echo 'Waiting for warden to start. Rechecking in 1 second'
  echo "Warden log contents"
  tail -n 200 '/tmp/warden.stdout.log'
  tail -n 200 '/tmp/warden.stderr.log'
  echo "*****************************"
  let sleeps--
  sleep 1
done

echo "/tmp/warden.sock exists, let's run the specs"

