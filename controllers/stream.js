const express = require("express");
const config = require('../config.js');
const redshift = require('.././services/redshift.js');
const kinesis = require('.././services/kinesis.js');
const s3 = require('.././services/s3.js');
const utils = require('.././services/utils.js');
const fs = require('fs');
const axios = require("axios").default;

const router = express.Router();

router.get("/merge", function (req, res) {
  s3.mergeFiles();
  res.send('Merging S3 files')
})


router.get("/aws/setup", function (req, res) {
  provisionAWSInfra().then((data) => {
    if (data != null && data.includes('Infra Created')) {
      res.send('AWS Infra successfully provisioned')
    }
  }).catch(function (error) {
    console.log('error ', error)
    res.send('Error provisioning AWS infra ');
  });
})

router.get("/kinesis", function (req, res) {
  kinesis.readSequentially();
  res.send('Read from Kinesis!')
});

router.post("/insert", function (req, res) {
  kinesis.createStream().then((streamName) => {
    console.log('streamName -- ',streamName);
    utils.sleep(20000).then(() => {
      recentSearch(req.body, null, 0, streamName);
    })
    res.send('Query Posted!')
  }).catch(function (error) {
    res.send('Error in the request')
  });
});

async function provisionAWSInfra() {
  // create Redshift serverless and tables
  return new Promise(function (resolve, reject) {
    redshift.createNamespaceCommand().then((nsData) => {
      redshift.createWorkgroup(nsData.namespace.namespaceName).then((wgData) => {
        createRedshiftTables();
        s3.createBucket().then((data) => {
        }).catch(function (error) {
          reject('Error creating bucket ', error);
        })
      }).catch(function (error) {
        reject('Error creating workgroup ', error);
      });
    }).catch(function (error) {
      reject('Error creating namespace ', error);
    })
  })
}

function createRedshiftTables() {
  console.log('Create Redshift tables')
  redshift.getWorkgroup().then((wgData) => {
    if (wgData.workgroup.status != 'AVAILABLE') {
      console.log('Waiting for Redshift Workgroup to get provisioned');
      // induce delay
      utils.sleep(30000);
      createRedshiftTables();
    } else if (wgData.workgroup.status === 'AVAILABLE')
      redshift.createTables();
  })
}

async function recentSearch(reqBody, nextToken, counter, streamName) {
  // validate requestBody before Search
  var rcntSearch = reqBody.recentSearch;
  let query = config.twitter.recentSearchAPI + '&query=' + rcntSearch.query + '&max_results=' + rcntSearch.maxResults;
  if (nextToken != undefined && nextToken != null)
    query = query + '&next_token=' + nextToken;
  if (rcntSearch.startTime != undefined && rcntSearch.startTime != null)
    query = query + '&start_time=' + rcntSearch.startTime;
  if (rcntSearch.endTime != undefined && rcntSearch.endTime != null)
    query = query + '&end_time=' + rcntSearch.endTime;
  //console.log('Recent search query : ', query);
  return new Promise(function (resolve, reject) {
    let userConfig = {
      method: 'get',
      url: query,
      headers: { 'Authorization': config.twitter.bearerToken }
    };
    axios(userConfig)
      .then(function (response) {
        if (response.data.data != null) {
          kinesis.putRecords(response.data, streamName);
          counter += 1;
        }
        if (response.data.meta != undefined && response.data.meta.next_token != undefined) {
          recentSearch(reqBody, response.data.meta.next_token, counter, streamName);
        }
        // end of search results
        if (response.data.meta != undefined && response.data.meta.next_token === undefined || response.data.meta.next_token === null) {
          // listen for Tweets
          kinesis.readSequentially(counter, rcntSearch, streamName);
        }
        resolve('Recent Search results are persisted in database');
      })
      .catch(function (error) {
        console.log('ERROR ', error.response.data);
        reject(error.response.data);
      });
  });
}

module.exports = router
