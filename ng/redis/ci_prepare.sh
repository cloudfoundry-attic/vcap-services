sudo gem install --no-ri --no-rdoc aws-sdk
TAR=redis.tar.gz
../../download_binaries_from_s3.rb $TAR
tar xf $TAR -C /
rm -f $TAR
