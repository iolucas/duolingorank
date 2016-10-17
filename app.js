var request = require("request")
var json2csv = require('json2csv');
var fs = require("fs");
var async = require("async");
var neo4j = require('node-neo4j');

//Utils
var print = console.log;

//Connect to db
var db = new neo4j('http://neo4j:lucas@localhost:7474');

//Define constraints
var modulePathConstraint = "CREATE CONSTRAINT ON (user:User) ASSERT user.username IS UNIQUE";

print("Initing db...")
//Execute constraint set query
db.cypherQuery(modulePathConstraint, function(err) {
    if(err)
        print(err);
    else {
        print("Db initiated");
        //main(process.argv);

        crawlUserCollection(["LucasVieir848760"], function() {
            console.log("DONE ALL");

        });
    }
});

var fields = [
    //'language_data',
    //'last_streak',
    //'upload-self-service',
    //'is_blocked_by',
    //'has_observer',
    'deactivated',
    'num_followers',
    //'is_following',
    //'calendar',
    //'tts_base_url_http',
    'id',
    //'dict_base_url',
    //'cohort',
    //'delete-permissions',
    'site_streak',
    'daily_goal',
    //'languages',
    'gplus_id',
    //'change-design',
    //'followers',
    //'inventory',
    //'is_self_observer',
    //'notif_event_ids',
    //'certificates',
    //'browser_language',
    //'events',
    //'location',
    'username',
    //'bio',
    //'tts_cdn_url',
    //'js_version',
    //'is_blocking',
    //'ui_language',
    'facebook_id',
    //'num_classrooms',
    //'blockers',
    //'is_observer',
    'learning_language_string',
    //'num_observees',
    //'is_follower_by',
    //'blocking',
    //'tts_base_url',
    //'following',
    //'trial_account',
    'created',
    'admin',
    //'transliterate',
    'learning_language',
    //'show_dashboard_ad',
    'twitter_id',
    //'freeze-permissions',
    'avatar',
    //'streak_extended_today',
    'rupees',
    'fullname',
    //'has_google_now',
    'num_following'
]


var userData = {}

var numberOfDone = 0;

function main() {

    var asyncQueue = async.queue(function(userUrl, callback) {

        console.log("Downloading data of " + userUrl + "...");

        //Download user data
        request("https://www.duolingo.com/users/" + userUrl, function(error, response, body) {
            if(error) {
                console.log(error);
                return;
            }

            var userObj = JSON.parse(body);

            //Create query to add user to db

            //Create query to create or update module data
            var createOrMatchQuery = "MERGE (n:User {username:'" + userObj.username +"'})";

            var createQuery = "";

            createQuery += "SET";
            for (var i = 0; i < fields.length; i++) {

                var key = fields[i];
                var value = userObj[key];

                if((typeof value) == 'string') {
                    value = '"' + value.trim() + '"';
                } else if((typeof value) == 'object') {
                    value = '"' + JSON.stringify(value) + '"';
                }

                createQuery += " n." + key + " = " + value + ", ";
            }
            createQuery = createQuery.substr(0, createQuery.length - 2);


            //Download user friendships data
            //https://www.duolingo.com/friendships/26560174
            request("https://www.duolingo.com/friendships/" + userObj.id, function(error, response, body) {
                if(error) {
                    console.log(error);
                    return;
                }

                var addUserQuery = "";

                var friendData = JSON.parse(body);

                //Iterate thru user followers
                for(var i = 0; i < friendData.followers.length; i++) {
                    var follower = friendData.followers[i].username;
                    addUserQuery += "MERGE (:User {username:'" + follower +"'}) ";

                    //Create query to add this user to the pendent users

                    // if(userData[follower] == undefined && numberOfDone < 5) {
                    //     userData[follower] = true;
                    //     asyncQueue.push(follower); //Push username to the queue
                    // }
                }

                //Iterate thru user followings
                for(var i = 0; i < friendData.following.length; i++) {
                    var following = friendData.following[i].username;
                    addUserQuery += "MERGE (:User {username:'" + following + "'}) ";

                    // if(userData[following] == undefined && numberOfDone < 5) {
                    //     userData[following] = true;
                    //     asyncQueue.push(following); //Push username to the queue
                    // }
                }

                var neoQuery = [createOrMatchQuery, createQuery, addUserQuery].join(" ");
                //console.log(neoQuery);

                console.log(neoQuery);

                console.log("Done with " + userUrl + ".");

                // callback();
                // return;

                // keep working on duolingo rank 
                // it wont take so much time and can return a lot of views 
                // create an async op to download and another to register on the db

                db.cypherQuery(neoQuery, function(err, result) {
                    callback();
                });

                //Push user data
                // userData[userObj.username] = userObj;

                // numberOfDone++;

                // callback();
            });

        });

    }, 5);

    //On queue is empty
    asyncQueue.drain = function() {

        var userDataArray = [];

        //Create user data array
        for(var user in userData) {
            userDataArray.push(userData[user]);
        }

        //Get csv string
        var csvResults = json2csv({ 
            data: userDataArray, 
            fields: fields
        });        

        //Write reponse on files
        fs.writeFileSync("data.csv", csvResults);

        console.log("DONE");
    }

    asyncQueue.push("douglaspin11");

}




//Crawl any collection of duolingo users urls
function crawlUserCollection(userCollection, callback) {

    var urlQty = userCollection.length;
    var downloadsLeft = userCollection.length;
    var doneQty = 0;

    print("Pages left: " + downloadsLeft);

    var userCollectionEmptyFlag = false;

    //Queue to handle addition of items into the database
    var databaseQueue = async.queue(function(userObj, taskCallback) {
        
        //Add this pageInfo data to the database
        addUserDataToDb(userObj, function(err) {
            taskCallback(err, userObj.username);
        });

    }, 1);

    //Callback to be called when the database queue are empty
    databaseQueue.drain = function() {
        console.log("Database queue is empty.");

        //If the url collection to be download is empty, call the finish callback
        if(userCollectionEmptyFlag)
            callback();
    }

    //Queue to handle the download of the wikipedia pages
    var userQueue = async.queue(function(userUrl, taskCallback) {

        console.log("Downloading data of " + userUrl + "...");

        //Download user data
        request("https://www.duolingo.com/users/" + userUrl, function(error, response, body) {
            downloadsLeft--;

            //If error, exit with it
            if(error) {
                taskCallback(getErrorString(error, 
                    "Crawl Error with user: '" + userUrl + "': "));
                return;
            }


            var userObj = JSON.parse(body);

            //Download user friendships data
            //https://www.duolingo.com/friendships/26560174
            request("https://www.duolingo.com/friendships/" + userObj.id, function(error, response, body) {
                if(error) {
                    console.log(error);
                    return;
                }

                var friendData = JSON.parse(body);

                //Add current user followers and following users
                userObj.followers = friendData.followers;
                userObj.following = friendData.following;

                //Push this pageinfo to the database queue
                databaseQueue.push(userObj, function(err, username) {
                    doneQty++;
                    if(err) {
                        var errorString = getErrorString(err, 
                            "Error while adding data to database from user " + articleTitle + ": ");
                        print(errorString);
                        writeErrorLog(errorString);
                    } else {
                        print("User '" + username + "' added to the database.");
                    } 
                    print("Users done: " + doneQty + "/" + urlQty);
                });

                //Call the task finish callback
                taskCallback(null, userUrl);

                //Push user data
                // userData[userObj.username] = userObj;

                // numberOfDone++;

                // callback();
            });

        });

    }, 1);

    //Callback to be called when the wikipages queue are empty
    userQueue.drain = function() {
        console.log("Users queue is empty.");
        userCollectionEmptyFlag = true;
    }

    userQueue.push(userCollection, function(err, userUrl){
        if(err) {
            var errorString = getErrorString(err, "Error while downloading page: " + userUrl);
            console.log(errorString);
            writeErrorLog(errorString);
        } else {
            print("User '" + userUrl + "' downloaded. Users left: " + downloadsLeft);
        }
    });
}


// addUserDataToDb(userObj, lang, function(err) {
//     taskCallback(err, userObj.username);
// });
function addUserDataToDb(userObj, callback) {
    //Create query to add user to db

    //Create query to create or update module data
    var createOrMatchQuery = "MERGE (n:User {username:'" + userObj.username +"'})";

    var createQuery = "";

    createQuery += "SET";
    for (var i = 0; i < fields.length; i++) {

        var key = fields[i];
        var value = userObj[key];

        if((typeof value) == 'string') {
            value = '"' + value.trim() + '"';
        } else if((typeof value) == 'object') {
            value = '"' + JSON.stringify(value) + '"';
        }

        createQuery += " n." + key + " = " + value + ", ";
    }
    createQuery = createQuery.substr(0, createQuery.length - 2);

    var addUserQuery = "";

    //Iterate thru user followers
    for(var i = 0; i < userObj.followers.length; i++) {
        var follower = userObj.followers[i].username;
        addUserQuery += "MERGE (:User {username:'" + follower +"'}) ";

        //Create query to add this user to the pendent users

        // if(userData[follower] == undefined && numberOfDone < 5) {
        //     userData[follower] = true;
        //     asyncQueue.push(follower); //Push username to the queue
        // }
    }

    //Iterate thru user followings
    for(var i = 0; i < userObj.following.length; i++) {
        var following = userObj.following[i].username;
        addUserQuery += "MERGE (:User {username:'" + following + "'}) ";

        // if(userData[following] == undefined && numberOfDone < 5) {
        //     userData[following] = true;
        //     asyncQueue.push(following); //Push username to the queue
        // }
    }

    var neoQuery = [createOrMatchQuery, createQuery, addUserQuery].join(" ");
    //console.log(neoQuery);

    //console.log(neoQuery);

    //console.log("Done with " + userUrl + ".");

    // callback();
    // return;

    // keep working on duolingo rank 
    // it wont take so much time and can return a lot of views 
    // create an async op to download and another to register on the db

    db.cypherQuery(neoQuery, function(err, result) {
        callback(err);
    });    
}










