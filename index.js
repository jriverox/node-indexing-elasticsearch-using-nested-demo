const repository = require('./mongoRepository');
const esWrapper = require('./elasticWrapper');
const batchSize = 5000;

async function execute (campania, tipoPersonalizacion, option ="parent"){
    let start = new Date();
    console.log(`ejecutando opcion: ${option} para la campaña: ${campania}, y el Tipo de personalizacion: ${tipoPersonalizacion}`);
    try {
        await syncEstrategias(campania, tipoPersonalizacion, option);
        
        await syncPersonalizaciones(campania, tipoPersonalizacion, option);
        let end = new Date();
        console.log(`Sinconizacion finalizada, tiempo total: ${end - start}`);
        
    } catch (error) {
        console.log(error);
    }
};

async function syncEstrategias(campania, tipoPersonalizacion, option ="parent"){
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
            if(option === "parent")
                await esWrapper.syncEstrategiasParentChild(estrategias);
            else
                await esWrapper.syncEstrategiasNested(estrategias);
            console.log(`Registros procesados ${processedCount}`);
        }
    }
}

async function syncPersonalizaciones(campania, tipoPersonalizacion, option ="parent"){
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
            if(option === "parent")
                await esWrapper.syncPersonalizacionesParentChild(personalizaciones);
            else
                await esWrapper.syncPersonalizacionesNested(personalizaciones);
            console.log(`Registros procesados ${processedCount}`);
        }
    }
}

async function checkDb(campania, tipoPersonalizacion){
    try {
        const query = {
            CodigoCampania: campania,
            TipoPersonalizacion: tipoPersonalizacion
        };
        let count = await repository.getCountEstrategias(query);
        console.log("Registros encontrados: ", count);
        
    } catch (error) {
        console.error(error);
    }
    
}
//execute("201911", "LAN", "nested");
checkDb("201911", "LAN");