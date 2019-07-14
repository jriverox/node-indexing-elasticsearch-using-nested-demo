// const data = require('./data');
// const es = require('elasticsearch');

// const indexName = 'blog';
// const indexType = '_doc';
// const indexUri = 'https://search-vpc-es-sbsearch-jrx-7oz3krusqunmql2g7kigmctl7u.us-east-1.es.amazonaws.com';

// let insertParents = () => {
//     let client = new es.Client({
//         host: indexUri
//     });
//     let body = [];

//     data.parents.forEach((item) => {
//         body.push(
//             { index:  { _index: indexName, _type: indexType, _id: item.id,  _routing : item.id } },
//             item
//         );
//     });
//     console.log(body);
//     client.bulk({
//         body
//       }, function (err, resp) {
//         if(err){
//             console.log(err);;
//         }
//         console.log(resp);        
//       });
// };

// let insertChildren = () => {
//     let client = new es.Client({
//         host: indexUri
//     });
//     let body = [];

//     data.children.forEach((item) => {
//         body.push(
//             { index:  { _index: indexName, _type: indexType, _id: item.id,  _routing : item.relation.parent } },
//             item
//         );
//     });
//     console.log(body);
//     client.bulk({
//         body
//       }, function (err, resp) {
//         if(err){
//             console.log(err);;
//         }
//         console.log(resp);        
//       });
// };

// insertParents();
// insertChildren();