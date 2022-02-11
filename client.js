const grpc = require('grpc');
var jsforce = require('jsforce');
const fs = require('fs');
require('dotenv').config({path: '../setup/.env'});
const path = require('path');
const avro = require('avro-js');
const sema = require('semaphore')(2);

var services = require('../protos/pubsub_api_grpc_pb.js'); //Enable the Services
var functions = require('../protos/pubsub_api_pb.js'); //Use this to extract all the related functions code marshalling



//Setup Channel Security
var rootcert = fs.readFileSync(path.resolve('../certs/sfdcapi.crt'));
const secureCreds = grpc.credentials.createSsl(rootcert);
//Create a Stub
var client = new services.PubSubClient('eventbusapi-core1.sfdc-ypmv18.svc.sfdcfc.net:7443', secureCreds);

//Include the fields from the env file.
const {CHANNEL, USERNAME, PASSWORD, TOKEN, URL, CLIENTID, CLIENTSECRET, REDIRECTURI} = process.env;
var sessionid;
var instanceurl;
var tenantid;

//Setup Custom Metadata Headers to be used in RPC Calls
const metaheader = new grpc.Metadata();
metaheader.add("x-sfdc-api-session-token", "00D5g000004SwSO!ARAAQDA7TEru8ajs98K_035yhpQF3zTgV6JyU5WhmsXt7qE0XZ1qIDSvXXHzf1Cgxlc7Yrc3v1IxGkAuwHALHThmKNLVfaEg");
metaheader.add("x-sfdc-instance-url", "https://playful-koala-lqrcxl-dev-ed.my.salesforce.com" );
metaheader.add("x-sfdc-tenant-id", "core/playful-koala-lqrcxl-dev-ed/00D5g000004SwSOEA0" );

async function ConnectoAuth(){
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

        }
    });
}

//Example of a Unary Call
async function getTopic(){

    var topic = CHANNEL.toString(); //Refer to the required data in the Process file.

    //Variable Initialization ONLY for Static approach
    var request = new functions.TopicRequest(); 
    request.setTopicName(topic);

    //Make the Unary Call
    client.getTopic(request, metaheader, (error, response) => {
        if(error){console.error(error);}
        else{
            console.log(response);
            console.log('This is the Schema Id ' + response.getSchemaId());
            console.log('This is the Topic Name ' + response.getTopicName());
            return response.getSchemaId();
        }
    })

}
//Example of BiDi Call
async function subscribetotopic(){

    //Setup the variables for the function
    var fetchRequest = new functions.FetchRequest();
    fetchRequest.setTopicName(CHANNEL.toString());
    fetchRequest.setReplayPreset(functions.ReplayPreset.EARLIEST);
    fetchRequest.setNumRequested(1);

    //Make a BiDi Call - Bidi Calls can be assinged to a variables to utilise more features like write, status, error, data, end etc.
    var call = client.subscribe(metaheader);
    
    call.write(fetchRequest); // in a BiDi call the client has to write the first stream to the server.
    
    call.on('data', (response) =>{
        console.log(response);
        const{wrappers_, array} = response;
        if(array.length){
            const array1 = array[0][0][0];
            console.log(array1);
            var schemaid = array1[1]; //Schema Id 
            var payload = array1[2]; // Msg Payload in Uint8Array format.
            var deco = decodedmsg(schemaid, payload); // function to decode the payload based on the schema.
        }
    });
    call.on('error', (error) =>{
        console.error(error);
    });
    call.on('metadata', (metadata) =>{
        console.log(metadata);
    });
    call.on('status', statusmsg =>{
        console.log(statusmsg);
    });
    

}
//Avro Function to Decode the message
function olddecodedmsg(schemaid, payload){

    //set the request format and the variable value
    var request = new functions.SchemaRequest();
    request.setSchemaId(schemaid);

    //Make the Unary call to get the schemaJSON
    client.getSchema(request, metaheader, (error, response) => {
        if(error){console.error(error);}
        else{

            const schemajson = response.getSchemaJson(); //function which gets the schmeaJSON from the response.
            const sch = JSON.parse(schemajson); // Parse the JSON Schema.
            console.log(sch);
            
            //Use the Avro Module to Decode the Msg using the retreived Schema
            const schema = avro.parse(sch); //Parse the JSON using Avro Function
            var buff = Buffer.from(new Uint8Array(payload)); // Convert the Buffer from Uint8Array to BufferArray
            console.log(buff);

            const decodedresponse = schema.fromBuffer(buff);
            console.log(decodedresponse);

            return decodedresponse; //Pass the response to other mehods for further use.
        }
    })

}

//Publish to Salesforce - Unary Call
async function publishtoorg(){
    
    //create the payload
    const payload = {
        "CreatedDate": new Date().getTime(),
        "CreatedById": '0055g000005Y4b0AAC',// Your user ID
        "OpportunityNumber__c" : {string: '0065g00000AlFbGAAV'},
        "OrderNumber__c":  {string: 'SK123456'}
    }
    
    var schemaid = "c_im_mhJYrDf2GB9DKDd6A"; // You can get the Schema Id or Make another Unary call to get the Schema Id
    var schemajson;
    var request = new functions.SchemaRequest();
    request.setSchemaId(schemaid);

    //Make the Unary call to get the schemaJSON
    client.getSchema(request, metaheader, (error, response) => {
        if(error){console.error(error);}
        else{
            //Encode the Payload
            schemajson = response.getSchemaJson();
            var schema = JSON.parse(schemajson);
            var sch = avro.parse(schema);
            var encodedpayload = sch.toBuffer(payload);
            console.log(encodedpayload);

                //create a Producer Event
            var prodEvent = new functions.ProducerEvent();
            prodEvent.setId("12345");
            prodEvent.setSchemaId(schemaid);
            prodEvent.setPayload(encodedpayload);
            
            var events = [prodEvent]; //Create an Array of Produce Events
            
            var pubRequest = new functions.PublishRequest();
            pubRequest.setTopicName("/event/OrderUpdate__e");
            pubRequest.setEventsList(events);
            
            //Make the Unary Call
            client.publish(pubRequest, metaheader, (error, pubResponse) => {
                if(error){console.error(error);}
                else{
                    console.log(pubResponse);
                }
            });

        }
    });

}

async function newsub(){

    //Setup the variables for the function
    var fetchRequest = new functions.FetchRequest();
    fetchRequest.setTopicName(CHANNEL.toString());
    fetchRequest.setReplayPreset(functions.ReplayPreset.EARLIEST);
    fetchRequest.setNumRequested(1);

    //Make a BiDi Call - Bidi Calls can be assinged to a variables to utilise more features like write, status, error, data, end etc.
    var call = client.subscribe(metaheader);
    
    call.write(fetchRequest); // in a BiDi call the client has to write the first stream to the server.
    
    call.on('data', (response) =>{
        console.log(response);
        const{wrappers_, array} = response;
        if(array.length){
            const array1 = array[0][0][0];
            console.log(array1);
            var schemaid = array1[1]; //Schema Id 
            var payload = array1[2]; // Msg Payload in Uint8Array format.
            var deco = decodedmsg(schemaid, payload); // function to decode the payload based on the schema.
        }
    });
    call.on('error', (error) =>{
        console.error(error);
    });
    call.on('metadata', (metadata) =>{
        console.log(metadata);
    });
    call.on('status', statusmsg =>{
        console.log(statusmsg);
    });
}

function decodedmsg(schemaid, payload){

    //set the request format and the variable value
    var request = new functions.SchemaRequest();
    request.setSchemaId(schemaid);

    //Make the Unary call to get the schemaJSON
    client.getSchema(request, metaheader, (error, response) => {
        if(error){console.error(error);}
        else{

            const schemajson = response.getSchemaJson(); //function which gets the schmeaJSON from the response.
            const sch = JSON.parse(schemajson); // Parse the JSON Schema.
            console.log(sch);
            
            //Use the Avro Module to Decode the Msg using the retreived Schema
            const schema = avro.parse(sch); //Parse the JSON using Avro Function
            var buff = Buffer.from(new Uint8Array(payload)); // Convert the Buffer from Uint8Array to BufferArray
            console.log(buff);

            const decodedresponse = schema.fromBuffer(buff);
            console.log(decodedresponse);

            return decodedresponse; //Pass the response to other mehods for further use.
        }
    })
}

function main(){
    //ConnectoAuth();
    //getTopic();
    //sema.take(subscribetotopic);
    //subscribetotopic();
    //newsub();
    publishtoorg();
}
main();