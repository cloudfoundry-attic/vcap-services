curl -H "AUTHORIZATION:$1" 127.0.0.1:8181/v2/$2 \
#  -d '{"name": "test_instance", "space_guid": "0f70b963-4325-4641-86ec-c0f1d1e4874d", "service_plan_guid": "5ccb704b-7bd7-41d3-9c83-8218296c378a"}'
# -d '{"label": "echo", "provider": "core", "token": "echo-token"}'
#-d '{"label": "echo-1.0", "provider": "core", "url": "http://127.0.0.1", "description": "yyy", "version": "1.0"}'
