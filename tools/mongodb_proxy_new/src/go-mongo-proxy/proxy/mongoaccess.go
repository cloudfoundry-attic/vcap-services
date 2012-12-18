package proxy

import (
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"strconv"
)

// Only one caller is allowed to invoked following functions method since we
// have a global variable here.
var session *mgo.Session

// singleton object instance
func startMongoSession(dbhost, port string) error {
	var err error
	newsession := false

	if session == nil {
		newsession = true
	} else {
		if err := session.Ping(); err != nil {
			session.Close()
			newsession = true
		}
	}

	if newsession {
		session, err = mgo.Dial(dbhost + ":" + port)
		if err != nil {
			logger.Error("Failed to connect %s:%s, [%s].", dbhost, port, err)
			return err
		}
	}
	return nil
}

func endMongoSession() {
	if session != nil {
		session.Close()
		session = nil
	}
}

// Should call 'startMongoSession' before this method.
//
// This function depends on output format of 'stats' command, it is united in
// all of current supported versions, 1.8, 2.0 and 2.2.
//
// Output example:
// {
//    "db" : "db",
//    "collections" : 17,
//    "objects" : 96,
//    "avgObjSize" : 2621535.5416666665,
//    "dataSize" : 251667412,
//    "storageSize" : 305500160,
//    "numExtents" : 20,
//    "indexes" : 15,
//    "indexSize" : 122640,
//    "fileSize" : 419430400,
//    "nsSizeMB" : 16,
//    "ok" : 1
// }
func readMongodbSize(dbname, user, pass string, size *float64) bool {
	var stats bson.M
	var temp float64

	*size = 0.0

	db := session.DB(dbname)
	err := db.Login(user, pass)
	if err != nil {
		logger.Error("Failed to login database db as %s:%s, [%s].", user, pass, err)
		return false
	}

	err = db.Run(bson.D{{"dbStats", 1}, {"scale", 1}}, &stats)
	if err != nil {
		logger.Error("Failed to get database %s stats [%s].", dbname, err)
		return false
	}

	if !parseMongodbStats(stats["dataSize"], &temp) {
		logger.Error("Failed to read db_data_size.")
		return false
	}
	*size += temp

	if !parseMongodbStats(stats["indexSize"], &temp) {
		logger.Error("Failed to read db_index_size.")
		return false
	}
	*size += temp

	logger.Debug("Get db data total size %v.", *size)
	return true
}

/*
 * NOTE: if disk data file gets very large, then the returned data size value would
 *       be encoded in 'float' format but not 'integer' format, such as
 *       2.098026476e+09, if we parse the value in 'integer' format then we get
 *       error. It always works if we parse an 'integer' value in 'float' format.
 */
func parseMongodbStats(value interface{}, result *float64) bool {
	temp, err := strconv.ParseFloat(fmt.Sprintf("%v", value), 64)
	if err != nil {
		logger.Error("Failed to convert data type: [%v].", err)
		return false
	}
	*result = temp
	return true
}
