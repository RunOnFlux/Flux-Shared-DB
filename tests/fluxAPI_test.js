const fluxAPI = require('../lib/fluxAPI');
const timer = require('timers/promises');
const md5 = require('md5');

async function testFluxAPI(){
  const DBAppName= "dbfluxtest4";
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
  for(let i=0; i<this.nodeInstances; i++){
    if(ipList[i].ip.includes(':')) ipList[i].ip = ipList[i].ip.split(':')[0];
    this.OpNodes.push({ip:ipList[i].ip, hash:md5(ipList[i].ip)});
  }
  console.log(`cluster ip's: ${JSON.stringify(this.OpNodes)}`);

  ip = await fluxAPI.getMyIp('65.108.109.25'.ip,33950);
  console.log(ip);
}
testFluxAPI();