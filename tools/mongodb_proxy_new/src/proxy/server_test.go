package main

import (
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"testing"
)

var config ProxyConfig

var proxy_started = false

func initTestConfig() {
	config.HOST = "127.0.0.1"
	config.PORT = "29017"

	config.MONGODB.HOST = "127.0.0.1"
	config.MONGODB.PORT = "27017"
	config.MONGODB.DBNAME = "db"
	config.MONGODB.USER = "admin"
	config.MONGODB.PASS = "123456"
}

func startTestProxyServer() {
	if !proxy_started {
		initTestConfig()
		go startProxyServer(&config)
		proxy_started = true
	}
}

func TestMongodbStats(t *testing.T) {
	startTestProxyServer()

	session, err := mgo.Dial(config.HOST + ":" + config.PORT)
	if err != nil {
		t.Errorf("Failed to establish connection with mongo proxy.\n")
	} else {
		defer session.Close()

		var stats bson.M

		db := session.DB("admin")
		err = db.Login(config.MONGODB.USER, config.MONGODB.PASS)
		if err != nil {
			t.Error("Failed to login database admin as %s:%s: [%s].",
				config.MONGODB.USER, config.MONGODB.PASS)
		}
		err = db.Run(bson.D{{"dbStats", 1}, {"scale", 1}}, &stats)
		if err != nil {
			t.Errorf("Failed to do dbStats command, [%v].\n", err)
		} else {
			fmt.Printf("Get dbStats result: %v\n", stats)
		}
	}
}

func TestMongodbDataOps(t *testing.T) {
	startTestProxyServer()

	session, err := mgo.Dial(config.HOST + ":" + config.PORT)
	if err != nil {
		t.Errorf("Failed to establish connection with mongo proxy.\n")
	} else {
		defer session.Close()

		db := session.DB("admin")
		err = db.Login(config.MONGODB.USER, config.MONGODB.PASS)
		if err != nil {
			t.Error("Failed to login database admin as %s:%s: [%s].",
				config.MONGODB.USER, config.MONGODB.PASS)
		}

		// 1. create collections
		coll := db.C("proxy_test")

		// 2. insert a new record
		err = coll.Insert(bson.M{"_id": "proxy_test_1", "value": "hello_world"})
		if err != nil {
			t.Errorf("Failed to do insert operation, [%v].\n", err)
		}

		// 3. query this new record
		result := make(bson.M)
		err = coll.Find(bson.M{"_id": "proxy_test_1"}).One(result)
		if err != nil {
			t.Errorf("Failed to do query operation, [%v].\n", err)
		} else {
			if result["value"] != "hello_world" {
				t.Errorf("Failed to do query operation.\n")
			} else {
				fmt.Printf("Get the brand new record: %v\n", result)
			}
		}

		// 4. update the new record's value
		err = coll.Update(bson.M{"_id": "proxy_test_1"}, bson.M{"value": "world_hello"})
		if err != nil {
			t.Errorf("Failed to do update operation, [%v].\n", err)
		} else {
			err = coll.Find(bson.M{"_id": "proxy_test_1"}).One(result)
			if err != nil {
				t.Errorf("Failed to do query operation, [%v].\n", err)
			} else {
				if result["value"] != "world_hello" {
					t.Errorf("Failed to do update operation.\n")
				}
			}
		}

		// 5. remove this new record
		err = coll.Remove(bson.M{"_id": "proxy_test_1"})
		if err != nil {
			t.Errorf("Failed to do remove operation, [%v].\n", err)
		} else {
			err = coll.Find(bson.M{"_id": "proxy_test_1"}).One(result)
			if err != nil {
				if err != mgo.ErrNotFound {
					t.Errorf("Failed to do remove operation, [%v].\n", err)
				}
			}
		}

		// 6. drop collection
		err = db.Run(bson.D{{"drop", "proxy_test"}}, nil)
		if err != nil {
			t.Errorf("Failed to drop collection, [%v].\n", err)
		}
	}
}
