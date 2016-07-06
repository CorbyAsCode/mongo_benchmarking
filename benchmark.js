//var collection = db.shardBatch;
var collection = db.shardRndBatch;
var docCount = 1000000;

function mysqlBatchInsert(n, tableName) {
  console.log("insert into " + tableName + " (t, n, a, v, j) values ");
  for(var i = 0; i < n; i++) {
    console.log("('2016-07-04'," + 7 * i + ",1,0,1234),");
    if (i+1 === n) {
      console.log("('2016-07-04'," + 7 * i + ",1,0,1234);");
    }
  }
}


function mysqlSequentialInsert(n, tableName) {
  for(var i = 0; i < n; i++) {
    var insert = "insert into " + tableName + " (t, n, a, v, j) ";
    var values = "values ('2016-07-04'," + 7 * i + ",1,0,1234);";
    var statement = insert + values;
    console.log(statement);
  }
}


function sequentialInsert(n, collection) {
  var startTime = getTime();
  var collStartCount = getCount(collection);
  for(var i = 0; i < n; i++) {
    collection.insert({ t: "2013-06-23", n: 7 * i, a: 1, v: 0, j: "1234" });
  }
  //db.getLastError();
  var finishTime = getTime();
  var collFinishCount = getCount(collection);
  var totalCount = calcDifference(collStartCount, collFinishCount);
  var elapsedTime = calcDifference(startTime, finishTime);
  var throughput = calcThrougput(totalCount, elapsedTime);
  printResults(totalCount, elapsedTime, throughput);
}


function batchInsert(n, collection) {
  var docs = [];

  for(var i = 0; i < n; i++) {
    docs.push({ t: "2013-06-23", n: 7 * i, a: 1, v: 0, j: "1234" });
  }

  var startTime = getTime();
  var collStartCount = getCount(collection);
  collection.insert(docs);
  //collection.getLastError();
  var finishTime = getTime();
  var collFinishCount = getCount(collection);
  var totalCount = calcDifference(collStartCount, collFinishCount);
  var elapsedTime = calcDifference(startTime, finishTime);
  var throughput = calcThroughput(totalCount, elapsedTime);
  printResults(totalCount, elapsedTime, throughput);
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


function batchInsertRnd(n, collection) {
  var docs = [];

  for(var i = 0; i < n; i++) {
    docs.push({ rnd: _rand(), t: "2013-06-23", n: 7 * i, a: 1, v: 0, j: "1234" });
  }
  // Why do we need this?
  //docs.sort(function(a, b) { return a.rnd - b.rnd; });

  var startTime = getTime();
  var collStartCount = getCount(collection);
  collection.insert(docs);
  //collection.getLastError();
  var finishTime = getTime();
  var collFinishCount = getCount(collection);
  var totalCount = calcDifference(collStartCount, collFinishCount);
  var elapsedTime = calcDifference(startTime, finishTime);
  var throughput = calcThroughput(totalCount, elapsedTime);
  printResults(totalCount, elapsedTime, throughput);
}

function getTime() {
  return Date.now();
}

function getCount(collection) {
  return parseInt(collection.count());
}

function calcDifference(start, finish) {
  return finish - start;
}

function calcThroughput(count, elapsed) {
  return count / elapsed * 1000;
}

function printResults(count, elapsed, throughput) {
  print(count + " documents inserted");
  print("Took " + elapsed + "ms");
  print(throughput + " docs/sec");
}

//var count0 = targetDB.find().count();
//var t0 = Date.now();

//mysqlBatchInsert(1000000, 'mongo_test');
//mysqlSequentialInsert(1000000, 'mongo_test');
// sequentialInsert(20000);
//batchInsert(docCount, collection);
// sequentialInsertRnd(20000);
batchInsertRnd(docCount, collection);

//var t1 = Date.now();
//var count1 = targetDB.find().count();

//var took = t1 - t0;
//var count = count1 - count0;
//var throughput = count / took * 1000;

//print(count + " documents inserted");
//print("Took " + took + "ms");
//print(throughput + " doc/s");

