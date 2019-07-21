require('dotenv').config();

const config = {
    elasticSearch: {
        uri: process.env.ELASTIC_URI,
        parentChildIndexName: "producto_parent_child",
        nestedIndexName: "producto_nested",
        denormalizedIndexName: "producto_denormalized",
        indexTpe: "_doc"
    },
    mongodb: {
        connectionString: process.env.MONGODB_URI,
        database: "costaricadb"
    }
};

module.exports = config;