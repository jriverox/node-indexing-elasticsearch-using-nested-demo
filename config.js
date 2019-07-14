require('dotenv').config();

const config = {
    elasticSearch: {
        uri: process.env.ELASTIC_URI,
        indexName: "producto_parent_child",
        indexTpe: "_doc"
    },
    mongodb: {
        connectionString: process.env.MONGODB_URI,
        database: "costaricadb"
    }
};

module.exports = config;