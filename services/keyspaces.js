const config = require('../config.js');
const cassandra = require('cassandra-driver')
const fs = require('fs')
const sigV4 = require('aws-sigv4-auth-cassandra-plugin')

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

const cassandraClient = new cassandra.Client({
    contactPoints: [host],
    localDataCenter: config.aws.env.region,
    authProvider: auth,
    sslOptions: sslOptions,
    protocolOptions: { port: 9142 },
    queryOptions: {
        consistency: cassandra.types.consistencies.localQuorum
    }
});

async function loadTweets() {
    const insertQuery = `INSERT INTO filtered_stream.tweets (id, text) VALUES(?,?)`
    let tweets = [
        ['202', 'tweet 2'],
        ['203', 'tweet 3']
    ]
    const results = await cassandra.concurrent.executeConcurrent(
        cassandraClient, insertQuery, tweets)
    console.log('Results', results)
}

module.exports = { loadTweets };
