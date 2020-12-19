var CONFIG = require('./config-prod.json');
const fetch = require('node-fetch');
const { URLSearchParams } = require('url');
const Realm = require('realm-web');
var assert = require('assert');
let psl = require('psl');

service_json = {
  "private_key": CONFIG.private_key,
  "client_email": CONFIG.client_email,
  "client_id": CONFIG.client_id
}

const Logger = console;

var jwt = require('jsonwebtoken');

async function getOAuthServiceToken(user) {
  var private_key = service_json.private_key; // private_key of JSON file retrieved by creating Service Account
  var client_email = service_json.client_email; // client_email of JSON file retrieved by creating Service Account
  var scopes = ["https://www.googleapis.com/auth/drive"]; // Scopes
  
  
  var url = "https://www.googleapis.com/oauth2/v3/token";

  var now = Math.floor(Date.now() / 1000);
  var claim = {
    iss: client_email,
    scope: scopes.join(" "),
    aud: url,
    exp: now + 3600,
    iat: now,
  };
  var token = jwt.sign(claim, private_key, { algorithm: 'RS256' });
  
  var p = {
      assertion: token,
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer"
  };
  const params = new URLSearchParams(p);

  var res = await fetch(url, { method: 'POST', body: params });
  var json = await res.json();
  
  return json.access_token;
}

async function getFileName(token, fid) {
  var u = `https://www.googleapis.com/drive/v3/files/${fid}?access_token=` + token  + "&supportsAllDrives=true&supportsTeamDrives=true";
  var r = await fetch(u);
  var j = await r.json();

  if (j.error) {
    console.log(`Error getting filename: `,j)
    throw j.error;
  }

  return j.name;
}

function getIdFromUrl(url) { return url.match(/[-\w]{25,}/); }

async function process_document(gtoken,project_id,document) {
  console.log(`Processing document ${document._id} for project ${project_id}`);
  console.log(document);

  let fid = getIdFromUrl(document.url)
  if (!fid) {
    console.log("Couldn't get file id")
    throw {error: "Couldn't get file id"};
  }

  let file_name = await getFileName(gtoken, fid)
  document.url_name = file_name;
}

function extractHostname(url) {
    var hostname;
    //find & remove protocol (http, ftp, etc.) and get hostname

    if (url.indexOf("//") > -1) {
        hostname = url.split('/')[2];
    }
    else {
        hostname = url.split('/')[0];
    }

    //find & remove port number
    hostname = hostname.split(':')[0];
    //find & remove "?"
    hostname = hostname.split('?')[0];

    return hostname;
}

function isGoogleUrl(url) {
  return psl.get(extractHostname(url)) === "google.com";
}

async function watcher(dbCollection) {
    console.log("Watcher start")
    const filter = {
          'fullDocument.documents' : {'$elemMatch':{url_name: null, url: {'$nin' : ['',null]}}}
        }

    var token = await getOAuthServiceToken();

    for await (let event of dbCollection.watch({filter})) {
        const {clusterTime, operationType, fullDocument} = event;
        //console.log(event);
        //console.log(event.fullDocument.documents)

        if ((operationType === 'insert' || operationType === 'update' || operationType === 'replace')
           && event.fullDocument.documents && (event.fullDocument.documents.length > 0))
         {
            let documents = event.fullDocument.documents;
            let newDocuments = [];
            let need_update = false;
            let project_id = event.fullDocument._id;

            for (let i in documents) {
              let doc = {...documents[i]};

              if (doc.url && !doc.url_name && isGoogleUrl(doc.url)) {
                try {
                  await process_document(token, project_id, doc)
                  need_update = true;
                } catch(err) {
                  if (err.code && (err.code != 404 /* Not found or we don't have access */)) {
                    //refresh token and try again
                    token = await getOAuthServiceToken();
                    try {
                      await process_document(token, project_id, doc)
                      need_update = true;
                    } catch(err) {
                      console.log("Exception processing document after a retry")
                      console.log(err)
                    }
                  }
                }
              }

              newDocuments.push(doc);
            }

            if (need_update) {
              let res = await dbCollection.updateOne({"_id" : project_id, "documents" : documents},{$set:{"documents":newDocuments}});
              console.log(`Updated documents for project ${project_id}: `, res)
            }
        }

        //only care about insert/update/replace
        //find matching report documents (there should be at least one) and process them

        // if ((operationType === 'insert' || operationType === 'update' || operationType === 'replace')
        //  && !event.fullDocument.processed) {
        //     const msg = event.fullDocument;

        //     let res = await dbCollection.updateOne({"_id" : msg._id, "processed" : false},{"$set":{"processed" : true, "ts_pickup" : new Date()}});
        //     if (res.modifiedCount > 0)
        //     {
        //       await process_message(dbCollection,msg);
        //     } else {
        //       console.log(`Watcher: someone else picked up the message ${msg._id}`)
        //     }
        // }
    }
  }

const realmApp = new Realm.App({ id: CONFIG.realmAppId });
const realmApiKey = CONFIG.realmApiKey;

async function loginApiKey(apiKey) {
  // Create an API Key credential
  const credentials = Realm.Credentials.apiKey(apiKey);
  // Authenticate the user
  const user = await realmApp.logIn(credentials);
  // `App.currentUser` updates to match the logged in user
  assert(user.id === realmApp.currentUser.id)
  return user
}

loginApiKey(realmApiKey).then(user => {
    console.log("Successfully logged in to Realm!");

    const dbCollection = user
      .mongoClient('mongodb-atlas')
      .db('shf')
      .collection('psproject');

    let timerId = setTimeout(async function watchForUpdates() {
        timerId && clearTimeout(timerId);
        await watcher(dbCollection);
        timerId = setTimeout(watchForUpdates, 5000);
    }, 5000);

  }).catch((error) => {
    console.error("Failed to log into Realm", error);
  });