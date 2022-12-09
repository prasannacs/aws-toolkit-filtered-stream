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
      if (data.Records != null) {
        data.Records.forEach(function (record, index) {
          //console.log('Data ',JSON.parse(record.Data))
          tweets.push(JSON.parse(record.Data));
        })
      }
      console.log('--- getRecords from Kiensis Stream --- ', tweets.length);
      if( tweets.length > 0 )  { /* && tweets.length < 51 */
        // execSync('sleep 3');
        // await redshift.insertTweets(tweets);
        counter = counter + tweets.length;
        s3.writeTweets(tweets);
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

  tweets = tweets.data;

  let records = [];
  if (tweets != null) {
    console.log('putRecords to Kinesis Stream -- ',tweets.length);
    tweets.forEach(function (tweet, index) {
      let record = {
        Data: Buffer.from(JSON.stringify(tweet)),
        PartitionKey: config.aws.kinesis.partitionKey
      }
      records.push(record);
    })
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

