pid=`ps aux | grep warden | grep -v 'bin/rake warden' | grep -v bash | grep -v grep | awk '{print $2}'`
if [ -n "$pid" ]
then 
  sudo echo $pid | xargs kill
fi
sudo rm -rf /tmp/warden ./warden/
