const config = require("./config");
const es = require('elasticsearch');

let syncEstrategias = async (estrategias) => {
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
            { index:  { _index: config.elasticSearch.indexName, _type: config.elasticSearch.indexTpe, _id: id,  _routing : id } },
            item
        );
    });
    //console.log(body);
    const bulkResponse = await client.bulk({
        body
    });
    //console.log(JSON.stringify(bulkResponse));
    
};

let syncPersonalizaciones = async (personalizaciones) => {
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
            { index:  { _index: config.elasticSearch.indexName, _type: config.elasticSearch.indexTpe, _id: id,  _routing : parentId } },
            doc
        );
    });
    //console.log(body);
    const bulkResponse = await client.bulk({
        body
    });
    //console.log(JSON.stringify(bulkResponse));
    
};

module.exports = {
    syncEstrategias,
    syncPersonalizaciones
}
