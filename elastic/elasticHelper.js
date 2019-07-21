const es = require('elasticsearch');
let client = null;
const config = require("../config");

let getElasticClient = () =>{
    if(!client){
        client = new es.Client({
            host: config.elasticSearch.uri
        });
    }
    return client;
}

let check = () => {
    getElasticClient.ping((error) => {
        if (error) {
          console.trace('elasticsearch cluster is down!');
        } console.log('All is well');
      });
}
module.exports = {
    getElasticClient,
    check
}