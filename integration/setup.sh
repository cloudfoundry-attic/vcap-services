cd integration
INTEGRATION_DIR=`pwd`

# clean up after the last run
./teardown.sh

mkdir -p tmp
mkdir -p log
if [[ ! -d tmp/cloud_controller_ng ]]; then
  git clone git@github.com:cloudfoundry/cloud_controller_ng.git tmp/cloud_controller_ng
fi

# set up the cloud controller
cd tmp/cloud_controller_ng
if [[ ! -z $CC_BRANCH ]]; then
  echo "Switching to CloudController branch $CC_BRANCH"
  git checkout $CC_BRANCH
fi

git submodule update --init
CCNG_DIR=`pwd`
bundle install
cp $INTEGRATION_DIR/test_cc.db /tmp/cloud_controller.db

# start nats server
echo "Starting NATS"
nats-server -d -P /tmp/nats-server.pid

# start cloud controller
echo "Starting CCNG"
bundle exec ./bin/cloud_controller >>$INTEGRATION_DIR/log/cc.log 2>>$INTEGRATION_DIR/log/cc.err &

# generate authentication token
export AUTHORIZATION_TOKEN=`bundle exec ruby $INTEGRATION_DIR/token.rb`
sleep 5

create_service_auth_token(){
  local label=$1
  local service_token=$2
  echo "CREATING service auth token for $label, $service_token"
  curl -H "AUTHORIZATION:$AUTHORIZATION_TOKEN" 127.0.0.1:8181/v2/service_auth_tokens -d "{\"label\":\"$label\", \"provider\":\"core\", \"token\":\"$service_token\"}" >/dev/null 2>/dev/null
}

create_service_auth_token mysql mysql-token
create_service_auth_token redis redis-token
create_service_auth_token postgresql postgresql-token
create_service_auth_token mongodb mongodb-token
create_service_auth_token rabbitmq rabbitmq-token
create_service_auth_token 'echo' 'echo-token'
curl -H "AUTHORIZATION:$AUTHORIZATION_TOKEN" 127.0.0.1:8181/v2/service_auth_tokens

# create organization and space
ORG_OUTPUT=`curl -H "AUTHORIZATION:$AUTHORIZATION_TOKEN" 127.0.0.1:8181/v2/organizations -d "{\"name\":\"test_org\"}" 2>/dev/null`
ORG_GUID=`ruby -rjson -e "puts JSON.parse('$ORG_OUTPUT')['metadata']['guid']"`
SPACE_OUTPUT=`curl -H "AUTHORIZATION:$AUTHORIZATION_TOKEN" 127.0.0.1:8181/v2/spaces -d "{\"name\":\"test_space\", \"organization_guid\":\"$ORG_GUID\"}" 2>/dev/null`
SPACE_GUID=`ruby -rjson -e "puts JSON.parse('$SPACE_OUTPUT')['metadata']['guid']"`

# start service redis
redis-server --port 5454 >>$INTEGRATION_DIR/log/redis.log 2>>$INTEGRATION_DIR/log/redis.err &

cd $INTEGRATION_DIR/../ng/mysql
bundle install

# start mysql gateway
bin/mysql_gateway -c $INTEGRATION_DIR/mysql_gateway.yml >>$INTEGRATION_DIR/log/mysql_gateway.log 2>>$INTEGRATION_DIR/log/mysql_gateway.err &
sleep 2
curl -H "AUTHORIZATION:$AUTHORIZATION_TOKEN" 127.0.0.1:8181/v2/services
PLAN_OUTPUT=`curl -H "AUTHORIZATION:$AUTHORIZATION_TOKEN" 127.0.0.1:8181/v2/service_plans 2>/dev/null`
PLAN_GUID=`ruby -rjson -e "puts JSON.parse('$PLAN_OUTPUT')['resources'].first['metadata']['guid']"`

# start mysql node
bin/mysql_node -c $INTEGRATION_DIR/mysql_node.yml >>$INTEGRATION_DIR/log/mysql_node.log 2>>$INTEGRATION_DIR/log/mysql_node.err &

sleep 5
# create mysql service instance
curl -H "AUTHORIZATION:$AUTHORIZATION_TOKEN" 127.0.0.1:8181/v2/service_instances -d "{\"name\":\"test_instance\", \"space_guid\":\"$SPACE_GUID\", \"service_plan_guid\":\"$PLAN_GUID\"}"
SERVICE_INSTANCE_OUTPUT=`curl -H "AUTHORIZATION:$AUTHORIZATION_TOKEN" 127.0.0.1:8181/v2/service_instances 2>/dev/null`
SERVICE_INSTANCE_GUID=`ruby -rjson -e "puts JSON.parse('$SERVICE_INSTANCE_OUTPUT')['resources'].first['metadata']['guid']"`

cat <<YAML >$INTEGRATION_DIR/setup_values.yml
---
  authorization_token: $AUTHORIZATION_TOKEN
  org_guid: $ORG_GUID
  space_guid: $SPACE_GUID
  plan_guid: $PLAN_GUID
  service_instance_guid: $SERVICE_INSTANCE_GUID
YAML
