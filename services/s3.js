const config = require('../config.js');
const AWS = require('aws-sdk');
const redshift = require('../services/redshift.js');
const utils = require('../services/utils.js');
const readLine = require('readline');

AWS.config.update({ region: config.aws.env.region });

// Create S3 service object
s3 = new AWS.S3({ apiVersion: '2006-03-01' });

async function createBucket() {
    let params = { Bucket: config.aws.s3.bucketName };
    return new Promise(function (resolve, reject) {
        s3.createBucket(params, function (err, data) {
            if (err) {
                console.log("Error", err);
                reject(err);
            } else {
                console.log("S3 Bucket " + config.aws.s3.bucketName + " created in", data.Location);
                resolve(data);
            }
        });

    })
}

async function deleteObject(fileName) {
    let params = { Bucket: config.aws.s3.bucketName, Key: fileName };
    return new Promise(function (resolve, reject) {
        s3.deleteObject(params, function (err, data) {
            if (err) {
                console.log("Error", err);
                reject(err);
            } else {
                console.log("S3 Bucket " + config.aws.s3.bucketName + " delete file", fileName, "data ", data);
                resolve(data);
            }
        });

    })
}

async function putObject(body, key) {
    let params = {
        Body: body,
        Bucket: config.aws.s3.bucketName,
        Key: key
    }
    s3.putObject(params, function (err, data) {
        if (err) console.log(err, err.stack);
        else console.log(data);
    })
}

async function writeTweets(tweets) {

    let sqlValues = '';
    let sqlEntityAtsVal = '';
    let sqlCtxAtsVal = ''
    let sqlHashVal = '';
    let sqlCashVal = '';
    let sqlMentionsVal = '';
    let sqlUrlsVal = '';
    let sqlRefTwtsVal = '';
    let ctx_annotations = [];
    if (tweets != null) {
        tweets.forEach(function (tweet, index) {
            // console.log('----');
            // console.log(tweet.entities);
            // console.log('----');
            let cDate = new Date(tweet.created_at);
            let cDateStr = cDate.getFullYear() + '-' + ("0" + (cDate.getMonth() + 1)).slice(-2) + '-' + ("0" + cDate.getDate()).slice(-2) + 'T' + ("0" + cDate.getHours()).slice(-2) + ':' + ("0" + cDate.getMinutes()).slice(-2) + ':' + ("0" + cDate.getSeconds()).slice(-2)
            let tweetURL = 'https://twitter.com/twitter/status' + tweet.id;
            sqlValues = sqlValues + tweet.id + '|' + utils.cleanseText(tweet.text) + '|' + tweet.author_id + '|' + tweet.conversation_id + '|' + cDateStr + '|' + 'null' + '|' + tweet.in_reply_to_user_id + '|' + tweet.lang + '|';
            sqlValues = sqlValues + tweet.public_metrics.like_count + '|' + tweet.public_metrics.reply_count + '|' + tweet.public_metrics.quote_count + '|' + tweet.public_metrics.retweet_count + '|'
            sqlValues = sqlValues + tweet.possibly_sensitive.toString() + '|' + tweet.reply_settings + '|' + tweet.source + '|' + tweetURL
            if (index + 1 < tweets.length) {
                sqlValues = sqlValues + '\n';
            }

            if (tweet.entities != null && tweet.entities.annotations != null) {
                if (tweet.entities.annotations.length > 0) {
                    tweet.entities.annotations.forEach(function (entity, index_entity) {
                        let entity_name = entity.normalized_text.replace(/(\r\n|\n|\r)/gm, "");
                        sqlEntityAtsVal = sqlEntityAtsVal + tweet.id + '|' + '0|' + entity.type + '|' + entity_name;
                        sqlEntityAtsVal = sqlEntityAtsVal + '\n';
                    })
                }
            }

            if (tweet.entities != null && tweet.entities.hashtags != null) {
                if (tweet.entities.hashtags.length > 0) {
                    tweet.entities.hashtags.forEach(function (hashtag, index) {
                        sqlHashVal = sqlHashVal + tweet.id + '|' + hashtag.tag;
                        sqlHashVal = sqlHashVal + '\n';
                    })
                }
            }
            if (tweet.entities != null && tweet.entities.cashtags != null) {
                if (tweet.entities.cashtags.length > 0) {
                    tweet.entities.cashtags.forEach(function (cashtag, index) {
                        sqlCashVal = sqlCashVal + tweet.id + '|' + cashtag.tag;
                        sqlCashVal = sqlCashVal + '\n';
                    })
                }
            }
            if (tweet.entities != null && tweet.entities.mentions != null) {
                if (tweet.entities.mentions.length > 0) {
                    tweet.entities.mentions.forEach(function (mention, index) {
                        sqlMentionsVal = sqlMentionsVal + tweet.id + '|' + mention.username + '|' + mention.id;
                        sqlMentionsVal = sqlMentionsVal + '\n';
                    })
                }
            }
            if (tweet.entities != null && tweet.entities.urls != null) {
                if (tweet.entities.urls.length > 0) {
                    tweet.entities.urls.forEach(function (url, index) {
                        sqlUrlsVal = sqlUrlsVal + tweet.id + '|' + url.url + '|' + url.expanded_url + '|' + url.display_url + '|' + url.status + '|' + url.title + '|' + 'desc|' + url.unwound_url;
                        sqlUrlsVal = sqlUrlsVal + '\n';
                    })
                }
            }

            if (tweet.context_annotations != undefined && tweet.context_annotations != null) {
                if (tweet.context_annotations.length > 0) {
                    tweet.context_annotations.forEach(function (context, index_ctx) {
                        let str = sqlCtxAtsVal + tweet.id + '|' + context.domain.id + '|' + context.domain.name + '|desc|' + context.entity.id + '|' + context.entity.name;
                        ctx_annotations.push(str);
                    })
                }
            }
        })
    }
    ctx_annotations = filterDomains(ctx_annotations);

    sqlCtxAtsVal = '';
    ctx_annotations.forEach(function (context, index) {
        sqlCtxAtsVal = sqlCtxAtsVal + context + '\n';
    })

    if( sqlValues.length > 0 )
        putObject(sqlValues, 'tweets-' + new Date().toISOString() + '.txt');
    if( sqlEntityAtsVal.length > 0 )
        putObject(sqlEntityAtsVal, 'entities_annotations-' + new Date().toISOString() + '.txt');
    if( sqlCtxAtsVal.length > 0 )
        putObject(sqlCtxAtsVal, 'context_annotations-' + new Date().toISOString() + '.txt');
    if( sqlHashVal.length > 0 )
        putObject(sqlHashVal, 'entities_hashtags-' + new Date().toISOString() + '.txt');
    if( sqlCashVal.length > 0 )
        putObject(sqlCashVal, 'entities_cashtags-' + new Date().toISOString() + '.txt');
    if( sqlMentionsVal.length > 0 )
        putObject(sqlMentionsVal, 'entities_mentions-' + new Date().toISOString() + '.txt');
    //putObject(sqlUrlsVal, 'entities_urls-' + new Date().toISOString() + '.txt');
}

function filterDomains(annotations) {
    let re = new RegExp("131|30|29")
    let newArray = []
    annotations.forEach(function (context, index_ctx) {
        if (re.test(context)) {
        } else {
            newArray.push(context);
        }
    })
    return newArray;
}

async function listObjects() {
    let params = {
        Bucket: config.aws.s3.bucketName,
        MaxKeys: 500
    };

    return new Promise(function (resolve, reject) {
        s3.listObjects(params, function (err, data) {
            if (err) {
                console.log(err, err.stack);
                reject(err);
            }
            else {
                //console.log(data);
                resolve(data);
            }
        })
    })
}

function mergeFiles() {
    let ctx_annotations = [];
    let tweets = [];
    let entities_annotations = [];
    let entities_cashtags = [];
    let entities_hashtags = [];
    let entities_mentions = [];

    listObjects().then((object) => {
        if (object != null && object.Contents.length > 0) {
            //console.log('S3 object -- ',object)
            object.Contents.forEach(function (content, index) {
                if (content.Key.startsWith('tweets-')) {
                    tweets.push(content.Key);
                }
                if (content.Key.startsWith('context_annotations-')) {
                    ctx_annotations.push(content.Key);
                }
                if (content.Key.startsWith('entities_annotations-')) {
                    entities_annotations.push(content.Key);
                }
                if (content.Key.startsWith('entities_cashtags-')) {
                    entities_cashtags.push(content.Key);
                }
                if (content.Key.startsWith('entities_hashtags-')) {
                    entities_hashtags.push(content.Key);
                }
                if (content.Key.startsWith('entities_mentions-')) {
                    entities_mentions.push(content.Key);
                }
            })
            aggregateRecords(tweets, 'tweets.txt');
            aggregateRecords(ctx_annotations, 'context-annotations.txt');
            aggregateRecords(entities_annotations, 'entities-annotations.txt');
            aggregateRecords(entities_mentions,'entities-mentions.txt');
            aggregateRecords(entities_hashtags,'entities-hashtags.txt');
            aggregateRecords(entities_cashtags,'entities-cashtags.txt');
        }
    })
}


async function aggregateRecords(fileArray, fileName) {
    let aggRecords = '';
    for (const [index1, file] of fileArray.entries()) {
        console.log('processing file ', file, ' | ', aggRecords.length);
        await getS3Contents(file).then((records) => {  
            for (const [index2, record] of records.entries()) {
                aggRecords = aggRecords + record + '\n';
                if (index1 === fileArray.length - 1 && index2 === records.length - 1) {
                    console.log('final file ', file, ' - ', aggRecords.length);
                    //return aggRecords;
                    putObject(aggRecords,fileName).then((data)    =>  {
                        redshift.copyCommand(fileName);
                        for(let file of fileArray ) {
                            deleteObject(file);
                        }
                    });

                }
            }
        }).catch(function (error)   {
            console.log('Cannot read contents from S3 file');
        })
    }
}

function getS3Contents(fileName) {
    let params = {
        Bucket: config.aws.s3.bucketName,
        Key: fileName
    }
    return new Promise((resolve, reject) => {
        let records = [];
        try {
            let readStream = s3.getObject(params).createReadStream();
            let lineReader = readLine.createInterface({ input: readStream });
            lineReader.on('line', line => {
                records.push(line);
            }).on('close', () => {
                resolve(records);
            });
        } catch (err) {
            console.log('Error: ', err);
            reject(err);
        }
    });
}

module.exports = { createBucket, putObject, writeTweets, listObjects, deleteObject, mergeFiles };

