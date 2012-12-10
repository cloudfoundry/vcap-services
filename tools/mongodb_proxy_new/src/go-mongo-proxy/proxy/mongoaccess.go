package proxy

import (
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"strconv"
)

var session *mgo.Session

// singleton object instance
func startMongoSession(dbhost, port string) error {
	var err error
	if session == nil {
		session, err = mgo.Dial(dbhost + ":" + port)
		if err != nil {
			return err
		}
	} else {
		if err = session.Ping(); err != nil {
			session.Close()

			session, err = mgo.Dial(dbhost + ":" + port)
			if err != nil {
				return err
			}
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

// should call 'startMongoSession' before this method
func readMongodbSize(dbname, user, pass string, size *float64) bool {
	var stats bson.M
	var temp float64

	*size = 0.0

	db := session.DB(dbname)
	err := db.Login(user, pass)
	if err != nil {
		logger.Error("Failed to login database db as %s:%s: [%s].", user, pass, err)
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
	db_data_size := temp
	*size += db_data_size

	if !parseMongodbStats(stats["indexSize"], &temp) {
		logger.Error("Failed to read db_index_size.")
		return false
	}
	db_index_size := temp
	*size += db_index_size

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
