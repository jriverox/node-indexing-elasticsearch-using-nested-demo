const config = require("./config");
const es = require('elasticsearch');

let syncEstrategiasParentChild = async (estrategias) => {
    let client = new es.Client({
        host: config.elasticSearch.uri
    });
    let body = [];

    estrategias.forEach((item) => {
        item.relation = {
            name: 'parent'
        };
        let id = `${item.CodigoCampania}.${item.CUV2}.${item.TipoPersonalizacion}`;
        
        body.push(
            { index:  { _index: config.elasticSearch.parentChildIndexName, _type: config.elasticSearch.indexTpe, _id: id,  _routing : id } },
            item
        );
    });
    //console.log(body);
    const bulkResponse = await client.bulk({
        body
    });
    //console.log(JSON.stringify(bulkResponse));
    
};

let syncPersonalizacionesParentChild = async (personalizaciones) => {
    let client = new es.Client({
        host: config.elasticSearch.uri
    });
    let body = [];

    personalizaciones.forEach((item) => {
        let parentId = `${item.AnioCampanaVenta}.${item.CUV}.${item.TipoPersonalizacion}`;
        let id = `${parentId}.${item.CodConsultora}.${item.DiaInicio}`;
 
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
    //console.log(body);
    const bulkResponse = await client.bulk({
        body
    });
    //console.log(JSON.stringify(bulkResponse));
    
};

let syncEstrategiasNested = async (estrategias) => {
    let client = new es.Client({
        host: config.elasticSearch.uri
    });
    let body = [];

    estrategias.forEach((item) => {

        let id = `${item.CodigoCampania}.${item.CUV2}.${item.TipoPersonalizacion}`;
        
        body.push(
            { index:  { _index: config.elasticSearch.nestedIndexName, _type: config.elasticSearch.indexTpe, _id: id } },
            item
        );
    });
    //console.log(body);
    const bulkResponse = await client.bulk({
        body
    });
    //console.log(JSON.stringify(bulkResponse));
    
};

let syncPersonalizacionesNested = async (personalizaciones) => {
    let client = new es.Client({
        host: config.elasticSearch.uri
    });
    let body = [];

    personalizaciones.forEach((item) => {
        let parentId = `${item.AnioCampanaVenta}.${item.CUV}.${item.TipoPersonalizacion}`;
        //let id = `${parentId}.${item.CodConsultora}.${item.DiaInicio}`;
 
        let personalizacion = {
            AnioCampanaVenta: item.AnioCampanaVenta,
            CUV: item.CUV,
            TipoPersonalizacion: item.TipoPersonalizacion,
            CodConsultora: item.CodConsultora,
            DiaInicio: item.DiaInicio,
            FlagRevista: item.FlagRevista,
            MaterialGanancia: item.MaterialGanancia
        };

        body.push(
            { update : {_id : parentId, _index : config.elasticSearch.nestedIndexName, _type: config.elasticSearch.indexTpe, retry_on_conflict : 3} },
            { doc: {personalizaciones : personalizacion}, doc_as_upsert : true }
        );
    });
    //console.log(body);
    const bulkResponse = await client.bulk({
        body
    });
    //console.log(JSON.stringify(bulkResponse));
    
};

module.exports = {
    syncEstrategiasParentChild,
    syncPersonalizacionesParentChild,
    syncEstrategiasNested,
    syncPersonalizacionesNested
}
