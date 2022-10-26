const AWS = require('aws-sdk');
const { CreateBucketCommand } = require("@aws-sdk/client-s3");
const { s3Client } = require("./libs/s3Client.js");
const { CreateClusterCommand } = require("@aws-sdk/client-redshift");
const { redshiftClient } = require("./libs/redshiftClient.js");
const { TimestreamWrite } = require("@aws-sdk/client-timestream-write")

// load AWS credentials
var credentials = new AWS.SharedIniFileCredentials({ profile: 'default' });
AWS.config.credentials = credentials;

const writeClient = new AWS.TimestreamWrite({ region: "us-east-1" });
const DATABASE_NAME = 'test1'
const TABLE_NAME = 'table3'

async function createDatabase() {
    console.log("Creating Database");
    const params = {
        DatabaseName: DATABASE_NAME
    };

    const promise = writeClient.createDatabase(params).promise();

    await promise.then(
        (data) => {
            console.log(`Database ${data.Database.DatabaseName} created successfully`);
        },
        (err) => {
            if (err.code === 'ConflictException') {
                console.log(`Database ${params.DatabaseName} already exists. Skipping creation.`);
            } else {
                console.log("Error creating database", err);
            }
        }
    );
}

async function createTable() {
    console.log("Creating Table");
    const params = {
        DatabaseName: DATABASE_NAME,
        TableName: TABLE_NAME,
        RetentionProperties: {
            MemoryStoreRetentionPeriodInHours: 2,
            MagneticStoreRetentionPeriodInDays: 1
        }
    };

    const promise = writeClient.createTable(params).promise();

    await promise.then(
        (data) => {
            console.log(`Table ${data.Table.TableName} created successfully`);
        },
        (err) => {
            if (err.code === 'ConflictException') {
                console.log(`Table ${params.TableName} already exists on db ${params.DatabaseName}. Skipping creation.`);
            } else {
                console.log("Error creating table. ", err);
                throw err;
            }
        }
    );
}

async function writeRecords() {
    console.log("Writing records");
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    const dimensions = [{'Name':'id', 'Value': '23423423245'},
                        {'Name':'lang', 'Value': 'en'},
                        {'Name':'text', 'Value': 'tweet text'}
                    ]

    let publicMetrics = {
        'Dimensions' : dimensions,
        'MeasureName' : 'publicMetrics',
        'MeasureValues' : [{'Name':'like_count','Type':'BIGINT','Value':'10'},
                            {'Name':'reply_count','Type':'BIGINT','Value':'11'},
                            {'Name':'retweet_count','Type':'BIGINT','Value':'1'},
                            {'Name':'quote_count','Type':'BIGINT','Value':'12'},
                            {'Name':'created_at','Type':'TIMESTAMP','Value': Date.now().toString()}],
        'MeasureValueType' : 'MULTI',
        'Time': currentTime.toString()
    }

    const annotations = {
        'Dimensions': dimensions,
        'MeasureName': 'context_entities',
        'MeasureValues': [{'Type':'VARCHAR','Name':'ctx_entity1', 'Value':'Halo'},
                            {'Type':'VARCHAR','Name':'ctx_entity2', 'Value':'Forza'}],
        'MeasureValueType': 'MULTI',
        'Time': currentTime.toString()
    };

    const records = [publicMetrics, annotations];

    const params = {
        DatabaseName: DATABASE_NAME,
        TableName: TABLE_NAME,
        Records: records
    };

    const request = writeClient.writeRecords(params);

    await request.promise().then(
        (data) => {
            console.log("Write records successful");
        },
        (err) => {
            console.log("Error writing records:", err);
            if (err.code === 'RejectedRecordsException') {
                const responsePayload = JSON.parse(request.response.httpResponse.body.toString());
                console.log("RejectedRecords: ", responsePayload.RejectedRecords);
                console.log("Other records were written successfully. ");
            }
        }
    );
}

//createDatabase();
//createTable();
writeRecords();