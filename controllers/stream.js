const express = require("express");
const config = require('../config.js');
const keyspaces = require('.././services/keyspaces.js');
const redshift = require('.././services/redshift.js');
const kinesis = require('.././services/kinesis.js');
const utils = require('.././services/utils.js');
const cassandra = require('cassandra-driver');
const sigV4 = require('aws-sigv4-auth-cassandra-plugin');
const fs = require('fs');
const axios = require("axios").default;
const axiosRetry = require("axios-retry");

const router = express.Router();

const auth = new sigV4.SigV4AuthProvider({
    region: config.aws.env.region,
    accessKeyId: config.aws.accessKeys.id,
    secretAccessKey: config.aws.accessKeys.secret
});

const host = 'cassandra.' + config.aws.env.region + '.amazonaws.com'
const sslOptions = {
    ca: [
        fs.readFileSync(__dirname + '/../resources/sf-class2-root.crt')
    ],
    host: host,
    rejectUnauthorized: true
};

const client = new cassandra.Client({
    contactPoints: [host],
    localDataCenter: config.aws.env.region,
    authProvider: auth,
    sslOptions: sslOptions,
    protocolOptions: { port: 9142 }
});

router.get("/", function (req, res) {
  provisionAWSInfra();
  res.send('AWS-Toolit');
});

router.get("/kinesis", function (req, res) {
  kinesis.getRecords();
  res.send('Kinesis Streams')
})

router.get("/cluster", function (req, res) {
  // redshift.createNamespaceCommand().then((nsData) =>  {
  //   redshift.createWorkgroup(nsData.namespace.namespaceName);
  // });
  redshift.insertTweetData();
  res.send('Query Posted!')
})

router.post("/insert", function (req, res) {
    recentSearch(req.body);
    res.send('Query Posted!')
});

async function provisionAWSInfra()  {
  // create Redshift serverless and tables
  redshift.createNamespaceCommand().then((nsData) =>  {
    redshift.createWorkgroup(nsData.namespace.namespaceName).then((wgData) =>  {
      createRedshiftTables();
    });
  })
}

function createRedshiftTables() {
  console.log('Create Redshift tables')
  redshift.getWorkgroup().then((wgData) =>  {
    if( wgData.workgroup.status != 'AVAILABLE') {
      console.log('Waiting for Redshift Workgroup to get provisioned');
      // induce delay
      utils.sleep(30000);
      createRedshiftTables();
    } else if ( wgData.workgroup.status === 'AVAILABLE' )
      redshift.createTables();
  })
}

async function recentSearch(reqBody, nextToken) {
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
            //redshift.insertTweetData(response.data);
            kinesis.putRecords(response.data);
          }
          if (response.data.meta != undefined && response.data.meta.next_token != undefined) {
            recentSearch(reqBody, response.data.meta.next_token);
          }
          resolve('Recent Search results are persisted in database');
        })
        .catch(function (error) {
          console.log('ERROR ',error.response.data);
          reject(error.response.data);
        });
    });
  }

module.exports = router
