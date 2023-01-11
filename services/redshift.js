const config = require('../config.js');

// Import required AWS SDK clients and commands for Node.js
const { RedshiftServerless, ConfigParameterFilterSensitiveLog } = require("@aws-sdk/client-redshift-serverless");
const AWS = require('aws-sdk');
const { aws } = require('../config.js');
var redshiftdata = new AWS.RedshiftData({ region: config.aws.env.region });

const client = new RedshiftServerless({ region: config.aws.env.region });

async function createNamespaceCommand() {
    let params = { 'adminUserPassword': config.aws.redshift.dbPassword, 'adminUsername': config.aws.redshift.dbUserName, 'namespaceName': config.aws.redshift.namespace, 'dbName': config.aws.redshift.dbName };
    return new Promise(function (resolve, reject) {
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
    let params = { 'namespaceName': namespaceName, 'workgroupName': config.aws.redshift.workgroup, 'baseCapacity': 32 };
    return new Promise(function (resolve, reject) {
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
    let params = { 'workgroupName': config.aws.redshift.workgroup };
    return new Promise(function (resolve, reject) {
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
    let sql_context_annotations = "CREATE table context_annotations (tweet_id VARCHAR(50) NOT NULL, domain_id VARCHAR(50), domain_name VARCHAR(500), domain_description VARCHAR(500), entity_id VARCHAR(50), entity_name VARCHAR(500), category VARCHAR(50), subcategory VARCHAR(50))"
    let sql_entities_annotations = "CREATE table entities_annotations (tweet_id VARCHAR(50) NOT NULL, probability INT, type VARCHAR(200), normalized_text VARCHAR(500), category VARCHAR(50), subcategory VARCHAR(50))"
    let sql_entities_cashtags = "CREATE table entities_cashtags (tweet_id VARCHAR(50) NOT NULL, cashtag VARCHAR(50), category VARCHAR(50), subcategory VARCHAR(50))"
    let sql_entities_hashtags = "CREATE table entities_hashtags (tweet_id VARCHAR(50) NOT NULL, hashtag VARCHAR(50), category VARCHAR(50), subcategory VARCHAR(50))"
    let sql_entities_mentions = "CREATE table entities_mentions (tweet_id VARCHAR(50) NOT NULL, username VARCHAR(50), user_id VARCHAR(50), category VARCHAR(50), subcategory VARCHAR(50))"
    let sql_referenced_tweets = "CREATE table referenced_tweets (tweet_id VARCHAR(50) NOT NULL, referenced_tweet_type VARCHAR(50), referenced_tweet_id VARCHAR(50), category VARCHAR(50), subcategory VARCHAR(50))"
    let sql_users = "CREATE table users(user_id VARCHAR(100) NOT NULL, name VARCHAR(1000) NOT NULL, username VARCHAR(100) NOT NULL, created_at DATETIME, description VARCHAR(2000), location VARCHAR(200), pinned_tweet_id VARCHAR(50), profile_image_url VARCHAR(300), protected BOOLEAN, followers_count INT, following_count INT, tweet_count INT, listed_count INT, url VARCHAR(200), verified BOOLEAN, PRIMARY KEY (user_id), category VARCHAR(50), subcategory VARCHAR(50))"
    let sql_tweets = "CREATE table tweets (tweet_id VARCHAR(100) NOT NULL, tweet_text VARCHAR(2000) NOT NULL, author_id VARCHAR(100), conversation_id VARCHAR(100), created_at DATETIME, geo_place_id VARCHAR(50), in_reply_to_user_id VARCHAR(50), lang VARCHAR(10), like_count INT, reply_count INT, quote_count INT, retweet_count INT, impression_count INT, possibly_sensitive BOOLEAN, reply_settings VARCHAR(20), source VARCHAR(150), tweet_url VARCHAR(100), PRIMARY KEY (tweet_id), category VARCHAR(50), subcategory VARCHAR(50))"

    let sqlArr = [];
    sqlArr.push(sql_context_annotations, sql_entities_annotations, sql_entities_cashtags, sql_entities_hashtags);
    sqlArr.push(sql_entities_mentions, sql_referenced_tweets, sql_users, sql_tweets);

    sqlArr.forEach(function (sqlQuery, index) {
        let params = { 'Database': config.aws.redshift.dbName, 'WorkgroupName': config.aws.redshift.workgroup, 'Sql': sqlQuery }
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

async function copyCommand(fileName) {
    let tableName = '';
    if (fileName.startsWith('tweets'))
        tableName = 'tweets'
    if (fileName.startsWith('context-annotations'))
        tableName = 'context_annotations'
    if (fileName.startsWith('entities-annotations'))
        tableName = 'entities_annotations'
    if (fileName.startsWith('entities-hashtags'))
        tableName = 'entities_hashtags'
    if (fileName.startsWith('entities-cashtags'))
        tableName = 'entities_cashtags'
    if (fileName.startsWith('entities-mentions'))
        tableName = 'entities_mentions'
    if (fileName.startsWith('users'))
        tableName = 'users'

    // let copySql = 'copy '+tableName+' from \'s3://'+config.aws.s3.bucketName+'/'+fileName+ '\' CREDENTIALS \'aws_access_key_id='+config.aws.secrets.aws_access_key_id+';aws_secret_access_key='+config.aws.secrets.aws_secret_access_key+'\''+' delimiter \'|\''+' region \''+config.aws.env.region + '\' timeformat '+'\'YYYY-MM-DDTHH:MI:SS\''; 
    let copySql = 'copy '+tableName+' from \'s3://'+config.aws.s3.bucketName+'/'+fileName+ '\' iam_role \'' + config.aws.iam.copyCmdPermission + '\' delimiter \'|\''+' region \''+config.aws.env.region + '\' timeformat '+'\'YYYY-MM-DDTHH:MI:SS\''; 
    let params = { 'Database': config.aws.redshift.dbName, 'WorkgroupName': config.aws.redshift.workgroup, 'Sql': copySql }
    console.log('copySql -- ',copySql);
    sqlExecute(params).then((data)  =>  {
    });
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

module.exports = { createNamespaceCommand, createTables, createWorkgroup, getWorkgroup, copyCommand };

