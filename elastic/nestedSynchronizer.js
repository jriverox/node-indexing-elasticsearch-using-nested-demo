const repository = require('../mongoRepository');
const config = require("../config");
const idGenerator = require('./idGenerartor');
const elasticHelper = require('./elasticHelper');
const batchSize = 100;

let execute = async (campaign, personalizationType) =>{
    await syncStrategies(campaign, personalizationType);
    //await syncPersonalizations(campaign, personalizationType);
}

let syncStrategies = async (campaign, personalizationType) => {
    const query = {
        CodigoCampania: campaign,
        TipoPersonalizacion: personalizationType
    };
    let count = await repository.getCountStrategies(query);
    let processedCount = 0;
    for (let page = 0; processedCount < count; page++) {
        console.log(count);
        let strategies = await repository.getStrategiesPaged(query, batchSize, page);
        processedCount += strategies.length;

        if(strategies.length > 0){
            for (let index = 0; index < strategies.length; index++) {
                let personalizations = await repository.getPersonalizationsByStrategy(campaign, personalizationType, strategies[index].CUV2);
                console.log("personalizaciones:", personalizations.length);
                if(personalizations.length > 0){
                    strategies[index].personalizaciones = [];
                    for (let i = 0; i < personalizations.length; i++) {
                        const item = personalizations[i];
                        let personalization = {                            
                            CodConsultora: item.CodConsultora,
                            DiaInicio: item.DiaInicio,
                            FlagRevista: item.FlagRevista,
                            MaterialGanancia: item.MaterialGanancia
                        };
                        strategies[index].personalizaciones.push(personalization);
                    }
                }
                await sendStrategiesToElastic(strategies, elasticHelper.getElasticClient());
            }
            //console.log(`Registros procesados ${processedCount}`);
        }
    }
}



async function sendStrategiesToElastic(strategies, elasticClient){

    try {
        let body = [];
    
        strategies.forEach((item) => {
    
            let id = idGenerator.getIdForStrategy(item.CodigoCampania, item.CUV2, item.TipoPersonalizacion);
            //console.log(id, item.personalizaciones);
            body.push(
                { index:  { _index: config.elasticSearch.nestedIndexName, _type: config.elasticSearch.indexTpe, _id: id} },
                item
            );
        });
        
        const bulkResponse = await elasticClient.bulk({
            body
        });
        //console.log(bulkResponse);
        
    } catch (error) {
        console.log(error);
    }
}

module.exports = {
    execute
}