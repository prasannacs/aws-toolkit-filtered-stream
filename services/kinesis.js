const { KinesisClient, AddTagsToStreamCommand } = require("@aws-sdk/client-kinesis");
const config = require('../config.js');
const AWS = require('aws-sdk');

const client = new KinesisClient({ region: config.aws.env.region });
AWS.config.apiVersions = {
    kinesis: '2013-12-02',
    // other service API versions
  };
  
  var kinesis = new AWS.Kinesis({ region: "us-east-1" });

async function createStream()   {
    let params = {
        StreamName: 'trending_tweets', /* required */
        ShardCount: '1',
        StreamModeDetails: {
          StreamMode: 'PROVISIONED' /* required */
        }
      };
      kinesis.createStream(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(data);           // successful response
      });
}

async function getRecords() {

  let params = {
    ShardId: 'shardId-000000000000', /* required */
    ShardIteratorType: 'TRIM_HORIZON',
    StreamName: 'trending_tweets', /* required */
    // StartingSequenceNumber: 'STRING_VALUE',
    Timestamp: new Date
  };
  kinesis.getShardIterator(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else  {
      console.log(data);           // successful response
      let params = {
        ShardIterator: data.ShardIterator, /* required */
        Limit: '100'
      };
      kinesis.getRecords(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(data);           // successful response
      });
    
    }     
  });

}

async function putRecords(tweets) {
  tweets = tweets.data;
  let records = [];
  if( tweets != null )  {
    tweets.forEach(function(tweet, index) {
      let record = {
        Data : Buffer.from(JSON.stringify(tweet)),
        PartitionKey: config.aws.kinesis.partitionKey
      }
      records.push(record);
    })
  }
  let params = {
    Records: records,
    StreamName: config.aws.kinesis.streamName /* required */
  };
  kinesis.putRecords(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else     console.log(data);           // successful response
  });
  
}

module.exports = { createStream, putRecords, getRecords };

