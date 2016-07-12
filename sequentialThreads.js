var Promise = require("bluebird");
var mongodb = require('mongodb');
var chance = require('chance');
var faker = require('faker');
var MongoClient = mongodb.MongoClient;
var Collection = mongodb.Collection;
Promise.promisifyAll(Collection.prototype);
Promise.promisifyAll(MongoClient);

var collection = 'shardSequentialInsert';
var docCount = 10;
var url = 'mongodb://localhost:27017/test';
var hostname = chance.domain();

var dataGenerator() {
  var data = {
    certname: hostname,
    pe_patch_version: 2,
    kernel: 'Linux',
    lsbdistdescription: "Red Hat Enterprise Linux Server release 6.7 (Santiago)",
    lsbdistrelease: 6.7,
    is_pe: true,
    swapsize_mb: 3072,
    sshfp_dsa: faker.lorem.sentence(),
    architecture: 'x86_64',
    platform_tag: 'el-6-x86_64',
    rubysitedir: "/opt/puppet/lib/ruby/site_ruby/1.9.1",
    hostname: hostname,
    pe_major_version: 3,
    productname: "VMware Virtual Platform",
    facterversion: "2.4.4",
    virtual: "vmware",
    pe_concat_basedir: "/var/opt/lib/pe-puppet/pe_concat",
    lsbdistcodename: "Santiago",
    rubyversion: "1.9.3",
    boardmanufacturer: "Intel Corporation",
    sshfp_rsa: faker.lorem.sentence(),
    bios_vendor: "Phoenix Technologies LTD",
    os: {
      family: "RedHat",
      lsb: {
        distcodename: "Santiago",
        distdescription: "Red Hat Enterprise Linux Server release 6.7 (Santiago)",
        distid: "RedHatEnterpriseServer",
        distrelease: "6.7",
        majdistrelease: "6",
        minordistrelease: "7",
        release: ":base-4.0-amd64:base-4.0-noarch:core-4.0-amd64:core-4.0-noarch"
      },
      name: "RedHat",
      release: {
        full: "6.7",
        major: "6",
        minor: "7"
      }
    },
    osfamily: "RedHat",
    ps: "ps -ef",
    augeasversion: "1.3.0",
    ipaddress_lo: "127.0.0.1",
    operatingsystem: "RedHat",
    manufacturer: "VMware, Inc.",
    pe_minor_version : 8,
    trusted: {
      authenticated: "remote",
      certname: hostname,
      extensions: {}
    },
    swapsize: "3.00 GB",
    bios_version: 6,
    network_lo: "127.0.0.0",
    fqdn: hostname,
    id : "root",
    hardwaremodel: "x86_64",
    netmask_lo: "255.0.0.0",
    uniqueid: "bb89893c",
    netmask: "255.255.255.0",
    lsbminordistrelease: 7,
    staging_http_get: "curl",
    kernelversion: "2.6.32",
    is_virtual: true,
    sshrsakey: faker.lorem.sentence(), 
    uuid: chance.guid(),
    sshdsakey: faker.lorem.sentence(), 
    operatingsystemrelease: 6.7,
    clientversion: "3.8.2 (Puppet Enterprise 3.8.2)",
    domain: chance.domain(),
    kernelmajversion: 2.6,
    rubyplatform: "x86_64-linux",
    hardwareisa: "x86_64",
    filesystems: "ext4,iso9660",
    mtu_lo: 65536,
    platform_symlink_writable: true,
    ipaddress: chance.ip(),
    puppetversion: "3.8.2 (Puppet Enterprise 3.8.2)",
    type: "Other",
    selinux: false,
    lsbmajdistrelease: 6,
    clientcert: hostname,
    lsbrelease: ":base-4.0-amd64:base-4.0-noarch:core-4.0-amd64:core-4.0-noarch",
    boardserialnumber: "None",
    operatingsystemmajrelease: 6,
    lsbdistid: "RedHatEnterpriseServer",
    pe_version: "3.8.2",
    gid: "root",
    custom_auth_conf: false,
    pe_build: "3.8.2",
    serialnumber: "VMware-42 06 ec 7c fb 5e cd b5-22 b4 08 01 ad 80 ed 58",
    boardproductname: "440BX Desktop Reference Platform",
    concat_basedir: "/var/opt/lib/pe-puppet/concat",
    vmwaretools_version: "9.10.0-2476743",
    sudoversion: "1.8.6p3",
    location: chance.state(),
    notes: false,
    project: "puppet",
    ocpm_installed: false,
    bash_vuln: false,
    project_url: chance.url(),
    dcnetwork: "standard",
    puppet_vardir: "/var/opt/lib/pe-puppet",
    rhn_enabled: false,
    root_home: "/root",
    has_infiniband: false,
    dependencies: false,
    infragroup: "prod",
    yumupdate_latest: false,
    gpfs_in_cluster: false,
    dmz: false,
    fc: false,
    simpana_installed: true,
    zenoss_server: "",
    ocum_installed: false,
    jenkins_plugins: "",
    mysql_running: false,
    postgres_running: true,
    simpana_bkup_success: true,
    memoryfree: "7.53 GB",
    last_run: "Wed Jun 22 10:27:13 EDT 2016",
    uptime_hours: 75,
    memoryfree_mb: 7715.33,
    system_uptime: {
      days: 3,
      hours: 75,
      seconds: 273311,
      uptime: "3 days"
    },
    uptime_seconds: 273311,
    timezone: "EDT",
    update_schedule: "prod_group_6",
    bios_release_date: chance.date(),
    esx_version: "unknown, please report 0xE9FE0",
    memorysize: chance.floating({min: 0, max: 100}),
    memorysize_mb: chance.floating({min: 0, max: 100});,
    partitions: {
      sda1 : {
        filesystem: "ext4",
        mount: "/boot",
        size: chance.integer({min:1000, max:48000}),
        uuid: chance.uuid()
      },
      sda2: {
          filesystem: "LVM2_member",
          size: chance.integer({min:1000, max:48000})
      },
      sdb1: {
          filesystem: "LVM2_member",
          size: chance.integer({min:1000, max:48000})
      },
      sdc1: {
          filesystem: "LVM2_member",
          size: chance.integer({min:1000, max:48000})
      },
      sdc2: {
          filesystem: "LVM2_member",
          size: chance.integer({min:1000, max:48000})
      },
      sdc3: {
          filesystem: "LVM2_member",
          size: chance.integer({min:1000, max:48000})
      }
    },
    clientnoop: false,
    blockdevices: "sda,sdb,sdc,sr0",
    processor11: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processor10: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processor13: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processor15: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    swapfree: "3.00 GB",
    physicalprocessorcount: 16,
    processors: {
      count: 16,
      models: [
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
        "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz"
      ],
      physicalcount: 16
    },
    processor14: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processor8: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processor9: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processor12: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processorcount: 16,
    swapfree_mb: 3072,
    macaddress: "00:01:02:03:04:05",
    netmask_eth2: "255.255.255.0",
    processor4: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    macaddress_eth2: "00:01:02:03:04:05",
    processor0: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    interfaces: "eth2,lo",
    ipaddress_eth2: chance.ip(),
    processor2: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    network_eth2: "137.187.60.0",
    processor7: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processor5: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processor6: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processor1: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    processor3: "Intel(R) Xeon(R) CPU E7-4830 v2 @ 2.20GHz",
    mtu_eth2: 1500,
    in_zenoss: true,
    nfs_mounts: false,
    kernel_latest: false,
    kernelrelease: "2.6.32-642.el6.x86_64",
    previously_installed_kernel: "2.6.32-573.22.1.el6.x86_64",
    uptime: "3 days",
    uptime_days: 3,
    recently_patched: false,
    yumsecurity_latest: true,
    path: "/sbin:/usr/sbin:/bin:/usr/bin"
  };

  return data;
}

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



