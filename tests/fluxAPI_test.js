/* eslint-disable */
const timer = require('timers/promises');
const md5 = require('md5');
const fluxAPI = require('../lib/fluxAPI');
const log = require('../lib/log');

async function testFluxAPI() {
  const DBAppName = 'dbfluxtest4';
  const Specifications = await fluxAPI.getApplicationSpecs(DBAppName);
  this.nodeInstances = Specifications.instances;
  // wait for all nodes to spawn
  let ipList = await fluxAPI.getApplicationIP(DBAppName);
  while (ipList.length < this.nodeInstances) {
    console.log(`Waiting for all nodes to spawn ${ipList.length}/${this.nodeInstances}...`);
    await timer.setTimeout(2000);
    ipList = await fluxAPI.getApplicationIP(DBAppName);
  }
  this.OpNodes = [];
  for (let i = 0; i < this.nodeInstances; i++) {
    if (ipList[i].ip.includes(':')) ipList[i].ip = ipList[i].ip.split(':')[0];
    this.OpNodes.push({ ip: ipList[i].ip, hash: md5(ipList[i].ip) });
  }
  console.log(`cluster ip's: ${JSON.stringify(this.OpNodes)}`);

  ip = await getMyIp(OpNodes);
  console.log(ip);
}
async function getMyIp(OpNodes, retries = 1) {
  const ipList = [];
  for (let i = 0; i < OpNodes.length && i < 5; i++) {
    let tempIp = await fluxAPI.getMyIp(OpNodes[i].ip, 33950);
    let j = 1;
    while (tempIp === null && j < 6) {
      console.log(`node ${OpNodes[i].ip} not responding to api port ${33950}, retrying ${j}/5...`);
      await timer.setTimeout(2000);
      tempIp = await fluxAPI.getMyIp(OpNodes[i].ip, 33950);
      j++;
    }
    if (tempIp !== null) ipList.push(tempIp);
  }
  // find the highest occurrence in the array
  if (ipList.length > 2) {
    const myIP = ipList.sort((a, b) => ipList.filter((v) => v === a).length - ipList.filter((v) => v === b).length).pop();
    return myIP;
  }
  console.log(`other nodes are not responding to api port ${33950}, retriying again...`);
  await timer.setTimeout(5000 * retries);
  return getMyIp(OpNodes, retries + 1);
}

async function testFluxAPI2() {
  const DBAppName = 'wordpressonflux';
  const startTime = new Date().getTime();
  const validity = await fluxAPI.validateApp(DBAppName, '185.136.186.202');
  const endTime = new Date().getTime();
  console.log(`validation time is ${endTime - startTime} milliseconds`);
  console.log(validity);
}
async function testFluxAPI3() {
  while(true){
    ipList = await fluxAPI.getApplicationIP('wordpress1678039648505');
    let ips = [];
    for (let i = 0; i < ipList.length; i++) {
      ips.push(ipList[i].ip);
    }
    log.info(ips);
    await timer.setTimeout(50000);
  }

}

//testFluxAPI();
//testFluxAPI2();
testFluxAPI3();

