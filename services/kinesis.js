const { KinesisClient, AddTagsToStreamCommand } = require("@aws-sdk/client-kinesis");
const config = require('../config.js');
const s3 = require('../services/s3.js');
const AWS = require('aws-sdk');
const utils = require('../services/utils.js');
const {execSync} = require('child_process');

const client = new KinesisClient({ region: config.aws.env.region });
AWS.config.apiVersions = {
  kinesis: '2013-12-02',
  // other service API versions
};

var kinesis = new AWS.Kinesis({ region: "us-east-1" });

async function createStream() {
  let params = {
    StreamName: config.aws.kinesis.streamName, /* required */
    ShardCount: '1',
    StreamModeDetails: {
      StreamMode: 'PROVISIONED' /* required */
    }
  };
  return new Promise(function (resolve,reject)  {
    kinesis.createStream(params, function (err, data) {
      if (err)  {
        console.log(err, err.stack); 
        reject(err);
      } 
      else  {
        console.log('Kinesis stream created ',data);
        resolve(data);
      } 
    });
  
  })
}

async function deleteStream() {
  let params = {
    StreamName: config.aws.kinesis.streamName, /* required */
    EnforceConsumerDeletion: true
  };
  return new Promise(function (resolve,reject)  {
    kinesis.deleteStream(params, function (err, data) {
      if (err)  {
        console.log(err, err.stack); 
        reject(err);
      } 
      else  {
        console.log('Kinesis stream created ',data);
        resolve(data);
      } 
    });
  
  })
}

async function readSequentially(resultCount) {
  let params = {
    ShardId: 'shardId-000000000000', /* required */
    ShardIteratorType: 'TRIM_HORIZON',
    StreamName: config.aws.kinesis.streamName,
    // StartingSequenceNumber: 'STRING_VALUE',
    Timestamp: new Date
  };
  kinesis.getShardIterator(params, function (err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else {
      console.log('Triggering GET RECORDS ',resultCount);
      getRecords(data.ShardIterator, resultCount, 0);
    }
  });
}

async function getRecords(shardIterator, resultCount, counter) {
  let params = {
    ShardIterator: shardIterator,
    Limit: '1000'
  };
  kinesis.getRecords(params, async function (err, data) {
    if (err) {
      console.log('Get Records Error ',err, err.stack); 
    }
    else {
      //console.log('Data -- ', data);
      let tweets = [];
      let users = [];
      let media = [];
      if (data.Records != null) {
        data.Records.forEach(function (record, index) {
          tweets = JSON.parse(record.Data).tweets;
          users = JSON.parse(record.Data).users;
          media = JSON.parse(record.Data).media;
          counter += 1;
          console.log('--- getRecords from Kiensis Stream --- ', counter);
          if( tweets.length > 0 )  { 
            s3.writeTweets(tweets);
          }   
          if( users.length > 0 )  {
            console.log('users -- ',users);
          }
        })
      }
      if( counter >= resultCount) {
        console.log('=== All Tweets copied to S3 ====');
        deleteStream();
      }
      if (data.NextShardIterator != null) {
        //console.log(' -data.NextShardIterator - ', data.NextShardIterator);
        getRecords(data.NextShardIterator, resultCount, counter);
      }
    }
  });
}

async function putRecords(tweets) {
  //console.log('USERS ',tweets.includes.users);

  // tweets = tweets.data;
  tweets = tweets.includes;

  let records = [];
  if (tweets != null) {
    console.log('putRecords to Kinesis Stream -- ',tweets.tweets.length);
//    tweets.forEach(function (tweet, index) {
      let record = {
        //Data: Buffer.from(JSON.stringify(tweet)),
        Data: Buffer.from(JSON.stringify(tweets)),
        PartitionKey: config.aws.kinesis.partitionKey
      }
      records.push(record);
//    })
  }
  let params = {
    Records: records,
    StreamName: config.aws.kinesis.streamName /* required */
  };
  kinesis.putRecords(params, function (err, data) {
    if (err) console.log(err, err.stack); 
    else {
      // console.log(data);           
    }
  });

}

module.exports = { createStream, putRecords, readSequentially };

