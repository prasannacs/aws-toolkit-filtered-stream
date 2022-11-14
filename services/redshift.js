const config = require('../config.js');
const utils = require('../services/utils.js');

// Import required AWS SDK clients and commands for Node.js
const { RedshiftServerless } = require("@aws-sdk/client-redshift-serverless");
const { CreateClusterCommand, waitUntilSnapshotAvailable } = require("@aws-sdk/client-redshift");
const { redshiftClient } = require("../libs/redshiftClient.js");
const { RedshiftServerlessClient } = require("@aws-sdk/client-redshift-serverless");
const AWS = require('aws-sdk');
var redshiftdata = new AWS.RedshiftData({ region: config.aws.env.region });

const client = new RedshiftServerless({ region: config.aws.env.region });

const params = {
    ClusterIdentifier: "Cluster1", // Required
    NodeType: "dc2.large", //Required
    MasterUsername: "master", // Required - must be lowercase
    MasterUserPassword: "Bond0505", // Required - must contain at least one uppercase letter, and one number
    ClusterType: "single-node", // Required
    //  IAMRoleARN: "IAM_ROLE_ARN", // Optional - the ARN of an IAM role with permissions your cluster needs to access other AWS services on your behalf, such as Amazon S3.
    //  ClusterSubnetGroupName: "CLUSTER_SUBNET_GROUPNAME", //Optional - the name of a cluster subnet group to be associated with this cluster. Defaults to 'default' if not specified.
    //  DBName: "DATABASE_NAME", // Optional - defaults to 'dev' if not specified
    //  Port: "PORT_NUMBER", // Optional - defaults to '5439' if not specified
};

async function createRedShiftCluster() {
    try {
        const data = await redshiftClient.send(new CreateClusterCommand(params));
        console.log(
            "Cluster " + data.Cluster.ClusterIdentifier + " successfully created"
        );
        return data; // For unit tests.
    } catch (err) {
        console.log("Error", err);
    }
};

async function createNamespaceCommand() {
    let params = { 'adminUserPassword': config.aws.redshift.dbPassword, 'adminUsername': config.aws.redshift.dbUserName, 'namespaceName': config.aws.redshift.namespace, 'dbName': config.aws.redshift.dbName };
    return new Promise(function(resolve, reject)    {
        client.createNamespace(params, function (error, data) {
            if (error) {
                console.log('Error ', error);
                reject(error);
            } else if (data) {
                console.log('Data ', data);
                resolve(data);
            }
        });
    
    });
}

async function createWorkgroup(namespaceName) {
    let params = { 'namespaceName': namespaceName, 'workgroupName' : config.aws.redshift.workgroup, 'baseCapacity' : 32 };
    return new Promise( function(resolve,reject)    {
        client.createWorkgroup(params, function (error, data) {
            if (error) {
                console.log('Error ', error);
                reject(error);
            } else if (data) {
                console.log('Data ', data);
                resolve(data);
            }
        });
    })
}

async function getWorkgroup() {
    let params = { 'workgroupName' : config.aws.redshift.workgroup };
    return new Promise( function(resolve,reject)    {
        client.getWorkgroup(params, function (error, data) {
            if (error) {
                console.log('Error ', error);
                reject(error);
            } else if (data) {
                console.log('Data ', data);
                resolve(data);
            }
        });
    })

}


async function createTables() {
    let sql_context_annotations = "CREATE table context_annotations (tweet_id VARCHAR(50) NOT NULL, domain_id VARCHAR(50), domain_name VARCHAR(50), domain_description VARCHAR(200), entity_id VARCHAR(50), entity_name VARCHAR(50))"
    let sql_entities_annotations = "CREATE table entities_annotations (tweet_id VARCHAR(50) NOT NULL, probability INT, type VARCHAR(20), normalized_text VARCHAR(50))"
    let sql_entities_cashtags = "CREATE table entities_cashtags (tweet_id VARCHAR(50) NOT NULL, cashtag VARCHAR(50))"
    let sql_entities_hashtags = "CREATE table entities_hashtags (tweet_id VARCHAR(50) NOT NULL, hashtag VARCHAR(50))"
    let sql_entities_mentions = "CREATE table entities_mentions (tweet_id VARCHAR(50) NOT NULL, username VARCHAR(50), user_id VARCHAR(50))"
    let sql_entities_urls = "CREATE table entities_urls (tweet_id VARCHAR(50) NOT NULL, url VARCHAR(100), expanded_url VARCHAR(200), display_url VARCHAR(200), status VARCHAR(50), title VARCHAR(200), description VARCHAR(500), unwound_url VARCHAR(500))"
    let sql_referenced_tweets = "CREATE table referenced_tweets (tweet_id VARCHAR(50) NOT NULL, referenced_tweet_type VARCHAR(50), referenced_tweet_id VARCHAR(50))"
    let sql_users = "CREATE table users(user_id VARCHAR(50) NOT NULL, name VARCHAR(100) NOT NULL, username VARCHAR(50) NOT NULL, created_at DATETIME, description VARCHAR(300), location VARCHAR(200), pinned_tweet_id VARCHAR(50), profile_image_url VARCHAR(300), protected VARCHAR(20), followers_count INT, following_count INT, tweet_count INT, listed_count INT, url VARCHAR(200), verified VARCHAR(20), PRIMARY KEY (user_id))"
    let sql_tweets = "CREATE table tweets (tweet_id VARCHAR(50) NOT NULL, tweet_text VARCHAR(280) NOT NULL, author_id VARCHAR(50), conversation_id VARCHAR(50), created_at DATETIME, geo_place_id VARCHAR(50), in_reply_to_user_id VARCHAR(50), lang VARCHAR(10), like_count INT, reply_count INT, quote_count INT, retweet_count INT, possibly_sensitive VARCHAR(10), reply_settings VARCHAR(20), source VARCHAR(100), tweet_url VARCHAR(100), PRIMARY KEY (tweet_id))"

    let sqlArr = [];
    sqlArr.push(sql_context_annotations, sql_entities_annotations, sql_entities_cashtags, sql_entities_hashtags);
    sqlArr.push(sql_entities_mentions, sql_entities_urls, sql_referenced_tweets, sql_users, sql_tweets);

    sqlArr.forEach(function(sqlQuery, index)    {
        let params = { 'Database': config.aws.redshift.dbName, 'WorkgroupName' : config.aws.redshift.workgroup, 'Sql' : sqlQuery }
        //console.log('SQL Query -- ',sqlQuery);
        redshiftdata.executeStatement(params, function (error, data) {
            if (error) {
                console.log('Error ', error);
            } else if (data) {
                console.log('Data ', data);
            }
        });
    })

}

async function insertUsers(users)   {
    let sqlValues = '';
    if( users != null ) {
        users.forEach(function(user,index)  {
            let cDate = new Date(user.created_at);
            sqlValues = sqlValues + '('+ user.id + ',' + '\'' + 'user.name' + '\'' + ',' + '\'' +  user.username + '\'' + ',' + '\'' + cDate.toUTCString() + '\'' + ',';
            sqlValues = sqlValues  + '\'' + 'user.description' + '\'' + ',' + '\'' + user.location + '\''  + ','+ '\'' + user.pinned_tweet_id + '\'' + ',' + '\'' + user.profile_image_url + '\''  + ','
            sqlValues = sqlValues  + '\'' + user.protected + '\'' + ',' +  user.public_metrics.followers_count + ',' + user.public_metrics.following_count + ',' + user.public_metrics.tweet_count + ',' + user.public_metrics.listed_count + ',' + '\'' + user.url + '\'' + ',' + '\'' + user.verified + '\'' + ')'
            sqlValues = sqlValues + ',';
        })
    }
    sqlValues = sqlValues.substring(0,sqlValues.length-1);
    let sqlUsers = 'INSERT INTO USERS (user_id,name,username,created_at,description,location,pinned_tweet_id,profile_image_url,protected,followers_count,following_count,tweet_count,listed_count,url,verified) VALUES ' + sqlValues;
    //console.log('SQL Users ',sqlUsers);
    let params = { 'Database': config.aws.redshift.dbName, 'WorkgroupName' : config.aws.redshift.workgroup, 'Sql' : sqlUsers }
    sqlExecute(params);

}

async function insertTweetData(searchData)    {
    insertTweets(searchData.data);
    insertUsers(searchData.includes.users);
}

async function insertTweets(tweets)    {
   
    let sqlValues = '';
    let sqlEntityAtsVal = '';
    let sqlCtxAtsVal = ''
    let sqlHashVal = '';
    let sqlCashVal = '';
    let sqlMentionsVal = '';
    let sqlUrlsVal = '';
    let sqlRefTwtsVal = '';
    let ctx_annotations = [];
    if( tweets != null )    {
        tweets.forEach(function(tweet,index)    {
            // console.log('----');
            // console.log(tweet.entities);
            // console.log('----');
            let cDate = new Date(tweet.created_at);
            let tweetURL = 'https://twitter.com/twitter/status' + tweet.id;
            sqlValues = sqlValues + '('+ tweet.id + ',' + '\'text\'' + ',' + tweet.author_id + ',' + '\'' +  cDate.toUTCString() + '\'' + ',' + '\'' + tweet.lang + '\'' + ',';
            sqlValues = sqlValues  + tweet.public_metrics.like_count + ',' + tweet.public_metrics.reply_count + ',' + tweet.public_metrics.retweet_count + ',' + tweet.public_metrics.quote_count + ',' + '\'' + tweetURL + '\'' + ','
            sqlValues = sqlValues  + '\'' + tweet.source + '\'' + ',' + '\'' + tweet.in_reply_to_user_id + '\''  + ','+ '\'' + tweet.possibly_sensitive.toString() + '\'' + ','+ '\'' + tweet.reply_settings + '\''  + ')'
            if( index+1 < tweets.length )   {
                sqlValues = sqlValues + ',';
            }

            if(tweet.entities != null && tweet.entities.annotations != null )   {
                if(tweet.entities.annotations.length > 0)   {
                    tweet.entities.annotations.forEach(function(entity,index_entity)   {
                    sqlEntityAtsVal = sqlEntityAtsVal + '(' + tweet.id + ',' + '\'' + entity.type + '\'' + ','+  '\'' + entity.normalized_text + '\'' + ')'
                    //if( index_entity+1 < tweet.entities.annotations.length )
                        sqlEntityAtsVal = sqlEntityAtsVal + ',';
                    })
                }
            }

            if( tweet.entities != null && tweet.entities.hashtags != null ) {
                if(tweet.entities.hashtags.length > 0)   {
                    tweet.entities.hashtags.forEach(function(hashtag,index) {
                        sqlHashVal = sqlHashVal + '(' + tweet.id + ',' + '\'' + hashtag.tag + '\'' + ')';
                        sqlHashVal = sqlHashVal + ',';
                    })
                }
            }
            if( tweet.entities != null && tweet.entities.cashtags != null ) {
                if(tweet.entities.cashtags.length > 0)   {
                    tweet.entities.cashtags.forEach(function(cashtag,index) {
                        sqlCashVal = sqlCashVal + '(' + tweet.id + ',' + '\'' + cashtag.tag + '\'' + ')';
                        sqlCashVal = sqlCashVal + ',';
                    })
                }
            }
            if( tweet.entities != null && tweet.entities.mentions != null ) {
                if(tweet.entities.mentions.length > 0)   {
                    tweet.entities.mentions.forEach(function(mention,index) {
                        sqlMentionsVal = sqlMentionsVal + '(' + tweet.id + ',' + '\'' + mention.username + '\'' + ',' + '\'' + mention.id + '\'' + ')';
                        sqlMentionsVal = sqlMentionsVal + ',';
                    })
                }
            }
            if( tweet.entities != null && tweet.entities.urls != null ) {
                if(tweet.entities.urls.length > 0)   {
                    tweet.entities.urls.forEach(function(url,index) {
                        sqlUrlsVal = sqlUrlsVal + '(' + tweet.id + ',' + '\'' + url.url + '\'' + ',' + '\'' + url.expanded_url + '\'' + ',' + '\'' + url.display_url + '\'' + ',' + '\'' + url.status + '\'' + ',' + '\'' + url.title + '\'' + ',' + '\'' + 'url.description' + '\'' + ',' + '\'' + url.unwound_url + '\'' +')';
                        sqlUrlsVal = sqlUrlsVal + ',';
                    })
                }
            }

            if(tweet.context_annotations != undefined && tweet.context_annotations != null)   {
                if( tweet.context_annotations.length > 0 )  {
                    tweet.context_annotations.forEach(function(context,index_ctx)   {  
                        let str = sqlCtxAtsVal + '(' + tweet.id + ',' + '\'' + context.domain.id + '\'' + ','+  '\'' + context.domain.name + '\'' + ',' + '\'' + context.entity.id + '\'' + ','+  '\'' + context.entity.name + '\'' + ')'   
                        ctx_annotations.push(str);
                    })
                }
            }
        })
    }
    ctx_annotations = filterDomains(ctx_annotations);

    sqlCtxAtsVal = '';
    ctx_annotations.forEach(function(context,index) {
        sqlCtxAtsVal = sqlCtxAtsVal + context + ',';
    })
    
    // remove the comma
    sqlEntityAtsVal = sqlEntityAtsVal.substring(0,sqlEntityAtsVal.length-1);
    sqlCtxAtsVal = sqlCtxAtsVal.substring(0,sqlCtxAtsVal.length-1);
    sqlHashVal = sqlHashVal.substring(0,sqlHashVal.length-1);
    sqlCashVal = sqlCashVal.substring(0,sqlCashVal.length-1);
    sqlMentionsVal = sqlMentionsVal.substring(0,sqlMentionsVal.length-1);
    sqlUrlsVal = sqlUrlsVal.substring(0,sqlUrlsVal.length-1);

    let sqlQuery = 'INSERT INTO TWEETS (tweet_id,tweet_text,author_id,created_at,lang, like_count,reply_count,retweet_count,quote_count,tweet_url, source, in_reply_to_user_id, possibly_sensitive, reply_settings) VALUES '+sqlValues;
    let sqlEntities = 'INSERT INTO ENTITIES_ANNOTATIONS (tweet_id,type,normalized_text) VALUES ' + sqlEntityAtsVal;
    let sqlContext = 'INSERT INTO CONTEXT_ANNOTATIONS (tweet_id,domain_id,domain_name,entity_id,entity_name ) VALUES' + sqlCtxAtsVal;
    let sqlHash = 'INSERT INTO ENTITIES_HASHTAGS (tweet_id,hashtag) VALUES ' + sqlHashVal;
    let sqlCash = 'INSERT INTO ENTITIES_CASHTAGS (tweet_id,cashtag) VALUES ' + sqlCashVal;
    let sqlMentions = 'INSERT INTO ENTITIES_MENTIONS (tweet_id,username,user_id) VALUES ' + sqlMentionsVal;
    let sqlUrl = 'INSERT INTO ENTITIES_URLS (tweet_id,url,expanded_url,display_url,status,title,description,unwound_url) VALUES ' + sqlUrlsVal;

    let sqlArr = [];
    sqlArr.push(sqlQuery, sqlEntities, sqlHash, sqlCash, sqlMentions, sqlUrl, sqlContext);

    sqlArr.forEach(function(sqlExec, index) {
        let params = { 'Database': config.aws.redshift.dbName, 'WorkgroupName' : config.aws.redshift.workgroup, 'Sql' : sqlExec }
        sqlExecute(params);
    })
}

function filterDomains(annotations)   {
    let re = new RegExp("('131')|('30')")
    let newArray = []
    annotations.forEach(function(context,index_ctx)   {
        if( re.test(context))   {
        }else{
            newArray.push(context);
        }
    })
    return newArray;
}

async function sqlExecute(params) {
    redshiftdata.executeStatement(params, function (error, data) {
        if (error) {
            console.log('SQL ', params.Sql + 'Error -- ', error);
        } else if (data) {
            //console.log('Data ', data);
        }
    });
}

module.exports = { createRedShiftCluster, createNamespaceCommand, createTables, createWorkgroup, getWorkgroup, insertTweetData };

