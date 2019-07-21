const mongoClient = require("mongodb").MongoClient;
const config = require("./config");

async function getClient(){
    return await mongoClient.connect(config.mongodb.connectionString, {useNewUrlParser: true});
}

let getCountStrategies = async (query) => {
    const client = await getClient();
    if(!client){
        return;
    }
    try {
        const db = client.db(config.mongodb.database);

        let collection  = db.collection("Estrategia");
        
        return await collection.countDocuments(query);
    } catch (error) {
        console.log(error);  
    } finally{
        client.close();
    }
};

let getStrategiesPaged = async (query, limit = 100, page=0) => {    
    // const client = await mongoClient.connect(config.mongodb.connectionString, {useNewUrlParser: true});

    const client = await getClient();
    if(!client){
        return;
    }
    try {
        const db = client.db(config.mongodb.database);

        let collection  = db.collection("Estrategia");
        let projection = { projection: { _id: 0, _idTipo: 0, Componentes: 0 }};

        return await collection.find(query, projection)
            .limit(parseInt(limit))
            .skip(parseInt(limit * page))
            .toArray();
    } catch (error) {
        console.log(error);  
    } finally{
        client.close();
    }
};

let getCountPersonalizations = async (query) => {
    const client = await getClient();
    if(!client){
        return;
    }
    try {
        const db = client.db(config.mongodb.database);

        let collection  = db.collection("OfertaPersonalizada");
        
        return await collection.countDocuments(query);
    } catch (error) {
        console.log(error);  
    } finally{
        client.close();
    }
};

let getPersonalizationsPaged = async (query, limit = 100, page=0) => {
    const client = await getClient();

        if(!client){
            return;
        }

        try {
            const db = client.db(config.mongodb.database)
            let collection  = db.collection("OfertaPersonalizada");
            let projection = {_id: 0, AnioCampanaVenta: 1, CUV: 1, TipoPersonalizacion: 1, CodConsultora: 1, DiaInicio: 1, FlagRevista: 1 };
            let sort = { CodConsultora: 1, CUV: 1 };
            return await collection.find(query, projection)
                .sort(sort)
                .limit(parseInt(limit))
                .skip(parseInt(limit * page))
                .toArray();

        } catch (error) {
          console.log(error);  
        } finally{
            client.close();
        }
};

let getPersonalizationsByStrategy = async (campaign, personalizationType, cuv) => {
    const client = await getClient();

    if(!client){
        return;
    }

    try {
        const db = client.db(config.mongodb.database)
        let collection  = db.collection("OfertaPersonalizada");
        let projection = {_id: 0, AnioCampanaVenta: 1, CUV: 1, TipoPersonalizacion: 1, CodConsultora: 1, DiaInicio: 1, FlagRevista: 1 };
        let sort = { CodConsultora: 1};
        let query = {
            AnioCampanaVenta: campaign,
            TipoPersonalizacion: personalizationType,
            CUV: cuv
        };

        return await collection.find(query, projection)
            .sort(sort)
            .toArray();

    } catch (error) {
        console.log(error);  
    } finally{
        client.close();
    }
};

module.exports = {
    getStrategiesPaged,
    getCountStrategies,
    getPersonalizationsPaged,
    getCountPersonalizations,
    getPersonalizationsByStrategy
};