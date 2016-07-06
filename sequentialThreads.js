var Promise = require("bluebird");
var mongodb = require('mongodb');
var MongoClient = Promise.promisifyAll(mongodb.MongoClient);

//var collection = db.shardBatch;
var collection = 'shardSequentialInsert';
var docCount = 10;
//var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/test';


//function sequentialInsert(n, collection) {
  //var startTime = getTime();
  //var collStartCount = getCount(collection);

  MongoClient.connectAsync(url)
    .then(function(db) {
      console.log('Connection established to', url);
      console.log('typeof db = ' + typeof db);
      return db.collection('batchInsert').find().toArrayAsync()
        .then(function(found) {
          console.log(found)
        })
        .finally(db.close());
    })
    .catch(console.log.bind(console));

       

      //console.log('collStartCount = ', collStartCount);
      /*var startTime = getTime();
      console.log('startTime = ' + startTime);
      
      var count = n;
      for(var i = 0; i < n; i++) {
        //console.log('i = ' + i + ', n = ' + n);
        var data = { t: "2013-06-23", n: 7 * i, a: 1, v: 0, j: "1234" }
        //console.log(data);
        coll.insert(data, function(err, result){
          if (err) {
            console.log(err);
          } else {
            count--;
            console.log(count);
            if (count == 0) {
              console.log('Finished');
              var collFinishCount = getCount(coll);
              var finishTime = getTime();
              var totalCount = calcDifference(collStartCount, collFinishCount);
              var elapsedTime = calcDifference(startTime, finishTime);
              var throughput = calcThroughput(totalCount, elapsedTime);
              printResults(totalCount, elapsedTime, throughput);
              db.close();
              return;
            }
          }
        });
      }
    }
  });
}*/


function sequentialInsertRnd(n, collection) {
  var startTime = getTime();
  var collStartCount = getCount(collection);
  for(var i = 0; i < n; i++) {
    collection.insert({ rnd: _rand(), t: "2013-06-23", n: 7 * i, a: 1, v: 0, j: "1234" });
  }
  //db.getLastError();
  var finishTime = getTime();
  var collFinishCount = getCount(collection);
  var totalCount = calcDifference(collStartCount, collFinishCount);
  var elapsedTime = calcDifference(startTime, finishTime);
  var throughput = calcThrougput(totalCount, elapsedTime);
  printResults(totalCount, elapsedTime, throughput);
}


function getCount(coll) {
  coll.count(function(err, result) {
    return parseInt(result);
  });
}


function getTime() {
  return new Date();
}


function calcDifference(start, finish) {
  return finish - start;
}

function calcThroughput(count, elapsed) {
  return count / elapsed * 1000;
}

function printResults(count, elapsed, throughput) {
  console.log(count + " documents inserted");
  console.log("Took " + elapsed + " ms");
  console.log(throughput + " docs/sec");
}

// sequentialInsertRnd(20000);
//batchInsertRnd(docCount, collection);

//sequentialInsert(docCount, collection); 
  /*
  var finishTime = new Date();
  var collFinishCount = getCount(coll);
  db.close();
  var totalCount = calcDifference(collStartCount, collFinishCount);
  var elapsedTime = calcDifference(startTime, finishTime);
  var throughput = calcThroughput(totalCount, elapsedTime);
  printResults(totalCount, elapsedTime, throughput);
});
*/
