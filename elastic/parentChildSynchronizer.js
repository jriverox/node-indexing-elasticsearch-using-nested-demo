const repository = require('../mongoRepository');
const config = require("../config");
const idGenerator = require('./idGenerartor');
const elasticHelper = require('./elasticHelper');
const batchSize = 1000;

let execute = async (campaign, personalizationType) =>{
    await syncStrategies(campaign, personalizationType);
    await syncPersonalizations(campaign, personalizationType);
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

        if(!strategies)
            throw new Error("estrategias nulo");

        if(strategies.length > 0){
            await sendStrategiesToElastic(strategies, elasticHelper.getElasticClient());
            console.log(`Registros procesados ${processedCount}`);
        }
    }
}


async function sendStrategiesToElastic(strategies, elasticClient){
    let body = [];

    strategies.forEach((item) => {
        item.relation = {
            name: 'parent'
        };
        let id = idGenerator.getIdForStrategy(item.AnioCampanaVenta, item.CUV, item.TipoPersonalizacion);
        
        body.push(
            { index:  { _index: config.elasticSearch.parentChildIndexName, _type: config.elasticSearch.indexTpe, _id: id,  _routing : id } },
            item
        );
    });
    //console.log(body);
    const bulkResponse = await elasticClient.bulk({
        body
    });
}

let syncPersonalizations = async (campaign, personalizationType) => {
    const query = {
        AnioCampanaVenta: campaign,
        TipoPersonalizacion: personalizationType
    };

    let count = await repository.getCountPersonalizations(query);
    let processedCount = 0;
    console.log(`Personalizaciones: ${count}`);

    for (let page = 0; processedCount < count; page++) {
        
        let personalizations = await repository.getPersonalizationsPaged(query, batchSize, page);
        processedCount += personalizations.length;
        if(!personalizations)
            throw new Error("estrategias nulo");
        if(personalizations.length > 0){
            await sendPersonalizationsToElastic(personalizations, elasticHelper.getElasticClient());
            console.log(`Registros procesados ${processedCount}`);
        }
    }
};

async function sendPersonalizationsToElastic(personalizations, elasticClient){
    let body = [];

    personalizations.forEach((item) => {
        let parentId = idGenerator.getIdForStrategy(item.AnioCampanaVenta, item.CUV, item.TipoPersonalizacion);
        let id = idGenerator.getIdForPersonalization(item.AnioCampanaVenta, item.CUV, item.TipoPersonalizacion, item.CodConsultora, item.DiaInicio);
 
        let doc = {
            AnioCampanaVenta: item.AnioCampanaVenta,
            CUV: item.CUV,
            TipoPersonalizacion: item.TipoPersonalizacion,
            CodConsultora: item.CodConsultora,
            DiaInicio: item.DiaInicio,
            FlagRevista: item.FlagRevista,
            MaterialGanancia: item.MaterialGanancia,
            relation: relation = {
                name: 'child',
                parent: parentId
            }
        };

        body.push(
            { index:  { _index: config.elasticSearch.parentChildIndexName, _type: config.elasticSearch.indexTpe, _id: id,  _routing : parentId } },
            doc
        );
    });
    const bulkResponse = await elasticClient.bulk({
        body
    });
    console.log(bulkResponse);
}

module.exports = {
    execute
}