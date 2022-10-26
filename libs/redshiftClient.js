// snippet-start:[redshift.JavaScript.createclientv3]
const { RedshiftClient } = require("@aws-sdk/client-redshift");
// Set the AWS Region.
const REGION = "us-east-1";
//Set the Redshift Service Object
const redshiftClient = new RedshiftClient({ region: REGION });
module.exports = { redshiftClient };
// snippet-end:[redshift.JavaScript.createclientv3]