const synchronizer = require('./synchronizerWrapper');
const elasticHelper = require('./elastic/elasticHelper');
const repository = require('./mongoRepository');
const config = require("./config");

async function run (campaign, personalizationType, option){
    let start = new Date();
    console.log(`ejecutando opcion: ${option} para la campaña: ${campaign}, y el Tipo de personalizacion: ${personalizationType}`);
    try {
        await synchronizer.execute(campaign, personalizationType, option);
        let end = new Date();
        console.log(`Sinconizacion finalizada, tiempo total: ${end - start}`);
        
    } catch (error) {
        console.log(error);
    }
};


async function checkMongoDb(campaign, personalizationType){
    try {
        const query = {
            CodigoCampania: campaign,
            TipoPersonalizacion: personalizationType
        };
        let count = await repository.getCountEstrategias(query);
        console.log("Registros encontrados: ", count);
        
    } catch (error) {
        console.error(error);
    }
    
}

function checkElasticsearch(){
    let client = elasticHelper.getElasticClient();
    client.ping((error) => {
        if (error) {
          console.trace('elasticsearch cluster is down!');
        } console.log('All is well');
      });
}
function test(p1, p2){
    console.log("test params", p1, p2);
}
/*
0 = denormalized
1 = parenChild
2 = nested
*/
//run("201911", "SR", 0);
run(process.argv[2], process.argv[3], process.argv[4]);
//checkDb("201911", "LAN");
//checkElasticsearch();
//test(process.argv[2], process.argv[3]);