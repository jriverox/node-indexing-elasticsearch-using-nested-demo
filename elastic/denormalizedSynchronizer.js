const repository = require('../mongoRepository');
const config = require("../config");
const idGenerator = require('./idGenerartor');
const elasticHelper = require('./elasticHelper');
const batchSize = 500;

let execute = async (campaign, personalizationType) =>{
    await syncPersonalizations(campaign, personalizationType);
    await syncStrategies(campaign, personalizationType);
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

        if(personalizations.length > 0){
            await sendPersonalizationsToElastic(personalizations, elasticHelper.getElasticClient());
            console.log(`Registros procesados ${processedCount}`);
        }
    }
};

async function sendPersonalizationsToElastic(personalizations, elasticClient){
    let body = [];

    personalizations.forEach((item) => {
        let id = idGenerator.getIdForPersonalization(item.AnioCampanaVenta, item.CUV, item.TipoPersonalizacion, item.CodConsultora, item.DiaInicio);
 
        let doc = {
            codigoCampania: item.AnioCampanaVenta,
            cuv: item.CUV,
            tipoPersonalizacion: item.TipoPersonalizacion,
            codigoConsultora: item.CodConsultora,
            diaInicio: item.DiaInicio,
            flagRevista: item.FlagRevista,
            materialGanancia: item.MaterialGanancia
        };

        body.push(
            { index:  { _index: config.elasticSearch.denormalizedIndexName, _type: config.elasticSearch.indexTpe, _id: id } },
            doc
        );
    });
    const bulkResponse = await elasticClient.bulk({
        body
    });
}

let syncStrategies = async (campaign, personalizationType) => {
    const query = {
        CodigoCampania: campaign,
        TipoPersonalizacion: personalizationType
    };
    let elasticClient = elasticHelper.getElasticClient();
    let count = await repository.getCountStrategies(query);
    let processedCount = 0;
    for (let page = 0; processedCount < count; page++) {
        console.log(count);
        let strategies = await repository.getStrategiesPaged(query, batchSize, page);
        processedCount += strategies.length;

        for (let index = 0; index < strategies.length; index++) {
            const strategy = strategies[index];
            await sendStrategyToElastic(strategy, elasticClient);        
        }
        console.log(`Registros procesados ${processedCount}`);
        
    }
}

function getQueryForUpdate(campaign, personalizationType, cuv){
    let query = {
        bool:{
            must:[
                {term: {codigoCampania: campaign}},
                {term: {cuv: cuv}},
                {term: {tipoPersonalizacion: personalizationType.toLowerCase()}}
            ]
        }
    };
    return query;
}
function replaceAll (str1, str2, ignore) 
{
    return str1.replace(new RegExp(str1.replace(/([\/\,\!\\\^\$\{\}\[\]\(\)\.\*\+\?\|\<\>\-\&])/g,"\\$&"),(ignore?"gi":"g")),(typeof(str2)=="string")?str2.replace(/\$/g,"$$$$"):str2);
} 

function getScriptForUpdateByQuery(strategy){
    
    let value = `ctx._source.Activo = ${strategy.Activo}; \
                ctx._source.CantidadPack = ${strategy.CantidadPack}; \
                ctx._source.CodigoEstrategia = ${strategy.CodigoEstrategia}; \
                ctx._source.CodigoProducto = '${strategy.CodigoProducto}'; \
                ctx._source.CodigoSAP = '${strategy.CodigoSAP}'; \
                ctx._source.CodigoTipoOferta = '${strategy.CodigoTipoOferta}'; \
                ctx._source.DescripcionCUV2 = '${strategy.DescripcionCUV2.split("'").join("")}'; \
                ctx._source.DescripcionTipoEstrategia = '${strategy.DescripcionTipoEstrategia}'; \
                ctx._source.EsSubCampania = ${strategy.EsSubCampania}; \
                ctx._source.EstrategiaId = ${strategy.EstrategiaId}; \
                ctx._source.Ganancia = ${strategy.Ganancia}; \
                ctx._source.ImagenEstrategia = '${strategy.ImagenEstrategia}'; \
                ctx._source.ImagenURL = '${strategy.ImagenURL}'; \
                ctx._source.IndicadorMontoMinimo = ${strategy.IndicadorMontoMinimo}; \
                ctx._source.LimiteVenta = ${strategy.LimiteVenta}; \
                ctx._source.MarcaDescripcion = '${strategy.MarcaDescripcion.split("'").join("")}'; \
                ctx._source.MarcaId = ${strategy.MarcaId}; \
                ctx._source.MatrizComercialId = ${strategy.MatrizComercialId}; \
                ctx._source.Orden = ${strategy.Orden}; \
                ctx._source.OrderTipoOferta = ${strategy.OrderTipoOferta}; \
                ctx._source.Precio = ${strategy.Precio}; \
                ctx._source.Precio2 = ${strategy.Precio2}; \
                ctx._source.PrecioPublico = ${strategy.PrecioPublico}; \
                ctx._source.TieneVariedad = ${strategy.TieneVariedad}; \
                ctx._source.TipoEstrategiaId = ${strategy.TipoEstrategiaId};`;
                
    return {
        inline: value
    }

}

async function sendStrategyToElastic(strategy, elasticClient){
    try {
        //console.log(strategy);
        let script = getScriptForUpdateByQuery(strategy);
        //console.log(script);
        let query = getQueryForUpdate(strategy.CodigoCampania, strategy.TipoPersonalizacion, strategy.CUV2);
        //console.log(JSON.stringify(query));
        let operationParams = { 
            index: config.elasticSearch.denormalizedIndexName,
            type: config.elasticSearch.indexTpe,
            body: { 
               query: query, 
               script: script
            },
            conflicts: 'proceed',
            timeout: '2m'
        
        }
        //console.log(JSON.stringify(operationParams));
        const response = await elasticClient.updateByQuery(operationParams);
        //console.log(response);        
    } catch (error) {
        console.log(error);
    }
}

module.exports = {
    execute
}