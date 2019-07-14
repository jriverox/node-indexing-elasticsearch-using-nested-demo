# Nodejs Demo about send data from MongoDB to Elasticsearch

## Demo Purpose
 This is a Demo of Synchronization Data from two collections in MongoDb and indexing to Elasticsearch using two approaches for relation the data of two collection. First using parent/child (join datatype) approach and second using nested (nested datatype)

### Details
When we sending data to elasticsearch the most common approach is to send data denormalize in a single index, in this case if your info is spread in two o more collection in mongo we have several alternatives:

1. Aggregate the data from two collections and send the result as flat documents to elastic.
2. Using join data type (parent/child approach) and send data of separate collection to different document in the same index, e.g: collection-Parent create document with relation field with relation.name = parent, and the other hand, data of collection-child is sended with relation field value relation.name = child and the both documents keep related. For more information please read https://dzone.com/articles/elasticsearch-parent-and-child-joins
2. Another approach is send data of collection-parent and after that, send data from collection-child in a Child field using nested data type https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html

### Notes
In config.js elasticSearch.uri and mongodb.connectionString depend on process.env.ELASTIC_URI and process.env.MONGODB_URI respectively. For the development purpose I'm using .env file (which was added to .gitignore for security reasons), then you must create .env file with de ELASTIC_URI and MONGODB_URI environment variables, example:

MONGODB_URI=mongodb+srv://youruser:yourpassword@yourhost
ELASTIC_URI=https://xxxx.es.amazonaws.com