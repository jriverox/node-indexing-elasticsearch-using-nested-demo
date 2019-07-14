const repository = require('./mongoRepository');
const esWrapper = require('./elasticWrapper');
const batchSize = 5000;

async function execute (campania, tipoPersonalizacion){
    let start = new Date();
    console.log(`ejecutando para la campaña: ${campania}, y el Tipo de personalizacion: ${tipoPersonalizacion}`);
    try {
        await syncEstrategias(campania, tipoPersonalizacion);
        
        await syncPersonalizaciones(campania, tipoPersonalizacion);
        let end = new Date();
        console.log(`Sinconizacion finalizada, tiempo total: ${end - start}`);
        
    } catch (error) {
        console.log(error);
    }
};

async function syncEstrategias(campania, tipoPersonalizacion){
    const query = {
        CodigoCampania: campania,
        TipoPersonalizacion: tipoPersonalizacion
    };
    let count = await repository.getCountEstrategias(query);
    let processedCount = 0;
    console.log(`Estratgias: ${count}`);
    for (let page = 0; processedCount < count; page++) {
        console.log(count);
        let estrategias = await repository.getEstrategias(query, batchSize, page);
        processedCount += estrategias.length;
        if(!estrategias)
            throw new Error("estrategias nulo");
        if(estrategias.length > 0){
            await esWrapper.syncEstrategias(estrategias);
            console.log(`Registros procesados ${processedCount}`);
        }
    }
}

async function syncPersonalizaciones(campania, tipoPersonalizacion){
    const query = {
        AnioCampanaVenta: campania,
        TipoPersonalizacion: tipoPersonalizacion
    };

    let count = await repository.getCountPersonalizaciones(query);
    let processedCount = 0;
    console.log(`Personalizaciones: ${count}`);

    for (let page = 0; processedCount < count; page++) {
        
        let personalizaciones = await repository.getPersonalizaciones(query, batchSize, page);
        processedCount += personalizaciones.length;
        if(!personalizaciones)
            throw new Error("estrategias nulo");
        if(personalizaciones.length > 0){
            await esWrapper.syncPersonalizaciones(personalizaciones);
            console.log(`Registros procesados ${processedCount}`);
        }
    }
}

execute("201911", "LAN");