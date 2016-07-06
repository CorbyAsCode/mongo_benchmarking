var Promise = require("bluebird");
var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var Collection = mongodb.Collection;

Promise.promisifyAll(Collection.prototype);
Promise.promisifyAll(MongoClient);

//var collection = db.shardBatch;
var collection = 'shardSequentialInsert';
var docCount = 10;
//var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/test';

var sequentialInserter = Promise.method(function(n, coll) {
  return new Promise(function(resolve, reject) {
    var count = n;
    for (var i = 0; i < n; i++) {
      var data = { t: "2013-06-23", n: 7 * i, a: 1, v: 0, j: "1234" };
      coll.insert(data, function(err, result) {
        if (err) {
          reject(err);
        } else {
          count--;
          if (count == 0) {
            console.log('Finished with sequential inserts');
            resolve(true);
          }
        }
      });
    }
  });
});

function promiseSeqInserts(n, collection) {
  MongoClient.connectAsync(url)
    .then(function(db) {
      var coll = db.collection(collection);
      var startTime = new Date();
      return coll.countAsync()
      .then(function(startCount) {
        return sequentialInserter(n, coll)
        .then(function() {
          console.log('Getting final count');
          return coll.countAsync()
          .then(function(finishCount) {
            var finishTime = new Date();
            var elapsed = finishTime - startTime;
            var totalInserts = finishCount - startCount;
            var rate = totalInserts / elapsed * 1000;
            console.log('elapsed time = ' + elapsed + ' ms');
            console.log('total inserted = ' + totalInserts);
            console.log('insert rate = ' + rate + ' docs/sec');
          });
        })
        .finally(function() {
        db.close()
        });
      })
    })
    .catch(function() {
      console.log.bind(console);
    });    
}

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

promiseSeqInserts(docCount, collection);



