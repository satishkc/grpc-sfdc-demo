const jsforce = require('jsforce');
require('dotenv').config();
//Include the fields from the env file.
const {CHANNEL, USERNAME, PASSWORD, TOKEN, URL, CLIENTID, CLIENTSECRET, REDIRECTURI} = process.env;

user = process.env.USERNAME;
pwd = process.env.PASSWORD;
tok = process.env.TOKEN;


var conn = new jsforce.Connection({
    oauth2:{
        loginUrl: URL,
        clientId: CLIENTID,
        clientSecret: CLIENTSECRET,
        redirectUri: REDIRECTURI
    }
});
conn.login(USERNAME, PASSWORD + TOKEN, function(error, AuthInfo){
    if(error){console.error(error);}
    else{
        console.log('Access Token : ' + conn.accessToken);
        console.log('Instance URL :' + conn.instanceUrl);
        console.log('User Id: '+ AuthInfo.id);
        console.log('Organization Id: '+ AuthInfo.organizationId);
    }
});