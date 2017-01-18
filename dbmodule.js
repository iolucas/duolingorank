var MongoClient = require('mongodb').MongoClient;
var moment = require("moment-timezone");

// Connection url
var url = 'mongodb://localhost:27017/duolingo';
//var url = "mongodb://46460205-f620-424d-b19f-3e47e5374fa5:81b56f61-adbe-434b-b5e5-cb2a01917423@192.155.243.53:10026/db";
//Create system to drop to a sqlite db in case no mongodb is accessible


var collectionRef = null;

module.exports = {

    init: function(callback) {
        // Connect using MongoClient
        MongoClient.connect(url, function(err, db) {
            // Create a collection we want to drop later
            collectionRef = db.collection('user');
            callback(err);
        });
    },

    insert: function(data, callback) {
        data.createdOn = moment().tz("America/Sao_Paulo").format('llll');

        collectionRef.insertOne(data, null, function(err, result) {
            callback(err);
        });
    },

    insertMany: function(docs, callback) {
        var createdOn = moment().tz("America/Sao_Paulo").format('llll');

        collectionRef.insertMany(docs, null, function(err, result) {
            callback(err, result);
        });
    },

    get: function(urlPath, callback) {
        collectionRef.findOne({path: urlPath},function(err, result) {
            callback(err, result);
        });
    },

    list: function(filter, callback) {
        collectionRef.find(filter).toArray(function(err, results) {
            callback(err, results);
        });
    }
}


// Connect using MongoClient
/*MongoClient.connect(url, function(err, db) {
    // Create a collection we want to drop later
    var col = db.collection('createIndexExample1');
    col.findOne({a:1},function(err, items) {
        console.log(items)
        //test.equal(null, err);
        //test.equal(4, items.length);
        db.close();
    });

});*/
/*
{
  "mongodb": [
    {
      "credentials": {
        "hostname": "192.155.243.53",
        "host": "192.155.243.53",
        "port": 10026,
        "username": "46460205-f620-424d-b19f-3e47e5374fa5",
        "password": "81b56f61-adbe-434b-b5e5-cb2a01917423",
        "name": "328ead99-b4fb-4198-a557-ae2b92342953",
        "db": "db",
        "url": "mongodb://46460205-f620-424d-b19f-3e47e5374fa5:81b56f61-adbe-434b-b5e5-cb2a01917423@192.155.243.53:10026/db"
      },
      "syslog_drain_url": null,
      "label": "mongodb",
      "provider": "core",
      "plan": "100",
      "name": "mongodb-6r",
      "tags": [
        "nosql",
        "document",
        "mongodb",
        "data_management",
        "ibm_experimental"
      ]
    }
  ]
}*/