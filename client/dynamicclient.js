const grpc = require('grpc');
var jsforce = require('jsforce');
const fs = require('fs');
require('dotenv').config({path: '../setup/.env'});
const path = require('path');
const avro = require('avro-js');
const sema = require('semaphore')(2);
const protoLoader = require("@grpc/proto-loader");
const { encode } = require('punycode');
const PROTO_PATH = "../protos/pubsub_api.proto";

const PackageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});

// Proto Package Namespace
const PubSubPackage = grpc.loadPackageDefinition(PackageDefinition).eventbus.v1;

// PubSub Api Endpoint
const PUBSUB_PORT = 'eventbusapi-core1.sfdc-ypmv18.svc.sfdcfc.net:7443';

//Setup Channel Security
var rootcert = fs.readFileSync(path.resolve('../certs/sfdcapi.crt'));
const secureCreds = grpc.credentials.createSsl(rootcert);

// Create the Pubsub Client
const stub = new PubSubPackage.PubSub(PUBSUB_PORT, secureCreds);

const {CHANNEL, USERNAME, PASSWORD, TOKEN, URL, CLIENTID, CLIENTSECRET, REDIRECTURI} = process.env;
var sessionid;
var instanceurl;
var tenantid;
var userid;
var schemaid;

  //Connect to Salesforce
var conn = new jsforce.Connection({
    oauth2 : {
    loginUrl : URL,
    clientId : CLIENTID,
    clientSecret : CLIENTSECRET,
    redirectUri : REDIRECTURI
    }
});
conn.login(USERNAME, PASSWORD + TOKEN, function(error, AuthInfo){
    if(error){console.error(error);}
    else{
        console.log('Access Token : ' + conn.accessToken);
        console.log('Instance URL :' + conn.instanceUrl);
        console.log('User Id: '+ AuthInfo.id);
        console.log('Organization Id: '+ AuthInfo.organizationId);

        sessionid = conn.accessToken;
        instanceurl = conn.instanceUrl;
        let t = instanceurl.split('/');
        let t1 = t[2].split('.my.salesforce.com');
        let myDomain = t1[0];
        tenantid = `core/${myDomain}/${AuthInfo.organizationId}`;
        userid = AuthInfo.id;

    }
});


// Update Authentication Details to be added to each call
const METADATA = {
    "x-sfdc-api-session-token": "00D5g000004SwSO!ARAAQBYYhfBfpzBuEw_t4n59Addjv7H9EjN1r.PwFjQziZ.xOE1YcTDGkyuQsdh2ksgWlVpIa8Ej_1yADOlLQtSobC83xj2H",
    "x-sfdc-instance-url": "https://playful-koala-lqrcxl-dev-ed.my.salesforce.com",
    "x-sfdc-tenant-id": "core/playful-koala-lqrcxl-dev-ed/00D5g000004SwSOEA0"
};

var topic_name = "/event/OppClosure__e";
//var topic_name = "/event/OrderUpdate__e";
//var schema_id = "c_im_mhJYrDf2GB9DKDd6A"; 

const authMetadata = new grpc.Metadata();
// Add metadata details
Object.keys(METADATA).forEach(key => {
    authMetadata.add(key, METADATA[key]);
});


//Get the Topic
stub.GetTopic({ "topic_name": topic_name }, authMetadata, (err, topicResponse) => {
    console.log('topicResponse', topicResponse);
    return topicResponse.schema_id;
});

//Subscribe to an event
var call = stub.Subscribe(authMetadata);
call.write({
    topic_name: topic_name,
    replay_preset: 'EARLIEST',
    num_requested: 2
});
call.on('data', (streamResponse) =>{
    const { events, latest_replay_id } = streamResponse;
    console.log(streamResponse);
    if (events && events.length) {
        const { event, replay_id } = events[0];
        const { schema_id, payload } = event;
        schemaid = schema_id;
        stub.GetSchema({ "schema_id": schema_id }, authMetadata, (err, schemaResponse) => {
            const schema_json = JSON.parse(schemaResponse.schema_json);
            const schema = avro.parse(schema_json);
            const decodedPayload = schema.fromBuffer(payload);
            const output = {
                schema: schema_id,
                payload: decodedPayload
            };
            console.log(output);

            /* for(const[key, value] of Object.entries(decodedPayload)){
                console.log(`${key} : ${JSON.stringify(value)}`);
            }; */
        });
    }    
});

//Publish the Payload

//Create the Payload JSON Object

/* const payloadstr = {
    "CreatedDate": new Date().getTime(),
    "CreatedById": '0055g000005Y4b0AAC',// Your user ID
    "OpportunityNumber__c" : {string: '0065g00000AlFbGAAV'},
    "OrderNumber__c":  {string: 'N123456'}
}
var schema_json;

var schemaRequest = {
    "schema_id" : schema_id
}

stub.GetSchema(schemaRequest, authMetadata, (err, schemaResponse) => {
    console.log(schemaResponse);
    schema_json = schemaResponse.schema_json;
    var schema = JSON.parse(schemaResponse.schema_json);
    console.log(schema);
    var sch = avro.parse(schema);
    const encodedpayload = sch.toBuffer(payloadstr);
    console.log(encodedpayload);

    var prodEvent = {
        "id": "1234",
        "schema_id": schema_id,
        "paylod": encodedpayload
    }
    console.log(prodEvent);

    var events = [prodEvent];
    const pubRequest = {
        "topic_name": topic_name,
        "events": events
    }
    stub.Publish(pubRequest, authMetadata, (err, pubResponse) =>{
        console.log(pubResponse);
    })

});
 */
