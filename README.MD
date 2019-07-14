# This is a Demo of Synchronization Data from two collections in MongoDb and indexing to Elasticsearch using two approaches for relation the data of two collection. First using parent/child (join datatype) approach and second using nested (nested datatype)

#Notes
In config.js elasticSearch.uri and mongodb.connectionString depend on process.env.ELASTIC_URI and process.env.MONGODB_URI respectively. For the development purpose I'm using .env file (which was added to .gitignore for security reasons), then you must create .env file with de ELASTIC_URI and MONGODB_URI environment variables, example:

MONGODB_URI=mongodb+srv://youruser:yourpassword@yourhost
ELASTIC_URI=https://xxxx.es.amazonaws.com