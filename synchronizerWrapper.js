
/**
 * 
 * @param {*} campaign 
 * @param {*} personalizationType 
 * @param {*} option = {0 = denormalized, 1 = parenChild, 2 = nested}
 */
let execute = async (campaign, personalizationType, option)=>{
    let elasticSynchronizer;
    opton = parseInt(option);
    switch (option) {
        case 1:
            elasticSynchronizer = require("./elastic/parentChildSynchronizer");
            break;
        case 2:
            elasticSynchronizer = require("./elastic/nestedSynchronizer");
            break;
        default:
            elasticSynchronizer = require("./elastic/denormalizedSynchronizer");
            break;
    }
    return await elasticSynchronizer.execute(campaign, personalizationType);
};


module.exports = {
    execute
};