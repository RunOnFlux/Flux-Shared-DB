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
    this.OpNodes.push({ip:ipList[i].ip, hash:md5(ipList[i].ip)});
  }
  console.log(`cluster ip's: ${JSON.stringify(this.OpNodes)}`);

  var ip = await fluxAPI.getMaster("38.242.243.3","33950");
  console.log(ip);
  ip = "::ffff:80.239.140.67";
  if(ip.includes(':')) ip = ip.split(':')[3];
  console.log(ip);

  ip = await fluxAPI.getMyIp("38.242.243.3","33950");
  console.log(ip);
}
testFluxAPI();