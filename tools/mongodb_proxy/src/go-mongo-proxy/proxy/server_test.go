package proxy

import (
    "testing"
    "syscall"
    "path/filepath"
    "fmt"
    "github.com/garyburd/go-mongo/mongo"
)
import l4g "github.com/moovweb/log4go"

var config ProxyConfig
var log l4g.Logger

var proxy_started = false

func initTestConfig() {
    config.HOST = "127.0.0.1"
    config.PORT = "29017"

    config.MONGODB.HOST   = "127.0.0.1"
    config.MONGODB.PORT   = "27017"
    config.MONGODB.DBNAME = "db"
    config.MONGODB.USER   = "admin"
    config.MONGODB.PASS   = "123456"

    config.FILTER.FS_RESERVED_BLOCKS = 0.5
    config.FILTER.INTERVAL           = 3
    config.FILTER.THRESHOLD          = 0.8
    config.FILTER.ENABLED            = true

    config.LOGGING.LEVEL = "info"
    config.LOGGING.PATH  = "/tmp/mongodb_proxy/proxy.log"
}

func initLog() {
    log_level := l4g.INFO
    log_path := config.LOGGING.PATH
    syscall.Mkdir(filepath.Dir(log_path), 0755)
    log = make(l4g.Logger)
    log.AddFilter("file", log_level, l4g.NewFileLogWriter(log_path, true))
}

func startTestProxyServer() {
    if !proxy_started {
        initTestConfig()
        initLog()
        go StartProxyServer(&config, log)
        proxy_started = true
    }
}

func TestMongodbStats(t *testing.T) {
    startTestProxyServer()

    conn, err := mongo.Dial(config.MONGODB.HOST + ":" + config.MONGODB.PORT)
    if err != nil {
        t.Errorf("Failed to establish connection with mongo proxy.\n")
    } else {
        defer conn.Close()

        var stats mongo.M

        db := mongo.Database{conn, "admin", mongo.DefaultLastErrorCmd}
        db.Authenticate(config.MONGODB.USER, config.MONGODB.PASS)

        err = db.Run(mongo.D{{"dbStats", 1}, {"scale", 1}}, &stats)
        if err != nil {
            t.Errorf("Failed to do dbStats command, [%v].\n", err)
        } else {
            fmt.Printf("Get dbStats result: %v\n", stats)
        }
    }
}

func TestMongodbDataOps(t *testing.T) {
    startTestProxyServer()

    conn, err := mongo.Dial(config.MONGODB.HOST + ":" + config.MONGODB.PORT)
    if err != nil {
        t.Errorf("Failed to establish connection with mongo proxy.\n")
    } else {
        defer conn.Close()

        db := mongo.Database{conn, "admin", mongo.DefaultLastErrorCmd}
        db.Authenticate(config.MONGODB.USER, config.MONGODB.PASS)

        // 1. create aollections
        coll := db.C("proxy_test")
        
        // 2. insert a new record
        err = coll.Insert(mongo.M{"_id": "proxy_test_1", "value": "hello_world"})
        if err != nil {
            t.Errorf("Failed to do insert operation, [%v].\n", err)
        }

        // 3. query this new record
        cursor, err := coll.Find(mongo.M{"_id": "proxy_test_1"}).Cursor()
        found := false
        for cursor.HasNext() {
            var m mongo.M
            err := cursor.Next(&m)
            if err != nil {
                t.Errorf("Failed to do query operation, [%v].\n", err)
            } else {
                fmt.Printf("Get the brand new record: %v\n", m)
            }
            if m["value"] == "hello_world" {
                found = true
            }
        }
        cursor.Close()
        if !found {
            t.Errorf("Failed to do query operations.\n")
        }

        // 4. update the new record's value
        err = coll.Update(mongo.M{"_id": "proxy_test_1"}, mongo.M{"value": "world_hello"})
        if err != nil {
            t.Errorf("Failed to do update operation, [%v].\n", err)
        } else {
            cursor, err = coll.Find(mongo.M{"_id": "proxy_test_1"}).Cursor()
            found := false
            for cursor.HasNext() {
                var m mongo.M
                err := cursor.Next(&m)
                if err != nil {
                    t.Errorf("Failed to do update operation, [%v].\n", err)
                } else {
                    fmt.Printf("Get the updated record: %v\n", m)
                }
                if m["value"] == "world_hello" {
                    found = true
                }
            }
            cursor.Close()
            if !found {
                t.Errorf("Failed to do update operations.\n")
            }
        }

        // 5. remove this new record
        err = coll.Remove(mongo.M{"_id": "proxy_test_1"})
        if err != nil {
            t.Errorf("Failed to do remove operation, [%v].\n", err)
        } else {
            cursor, err = coll.Find(mongo.M{"_id": "proxt_test_1"}).Cursor()
            found := false
            for cursor.HasNext() {
                var m mongo.M
                err := cursor.Next(&m)
                if err != nil {
                    t.Errorf("Failed to do remove operation, [%v].\n", err)
                }
                if m["value"] == "world_hello" {
                    found = true
                }
            }
            cursor.Close()
            if found {
                t.Errorf("Failed to do remove operations.\n")
            }
        }

        // 6. drop collection
        err = db.Run(mongo.D{{"drop", "proxy_test"}}, nil)
        if err != nil && err.Error() != "ns not found" {
            t.Errorf("Failed to drop collection, [%v].\n", err)
        }
    }
}
