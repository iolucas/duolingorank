var json2csv = require('json2csv');
var fs = require("fs");
var neo4j = require('node-neo4j');

//Utils
var print = console.log;

//Connect to db
var db = new neo4j('http://neo4j:lucas@localhost:7474');

var neoQuery = "MATCH (u:User) WHERE u.id IS NOT NULL RETURN u";


//Execute constraint set query
db.cypherQuery(neoQuery, function(err, result) {
    if(err)
        print(err);
    else {
        //Get fields
        var fields = [];

        for(var prop in result.data[0])
            fields.push(prop);

        //Get csv string
        var csvResults = json2csv({ 
            data: result.data, 
            fields: fields
        });        

        //Write reponse on files
        fs.writeFileSync("users.csv", csvResults);

        console.log("EXPORTING DONE");
    }
});