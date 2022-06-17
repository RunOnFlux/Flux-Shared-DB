const fluxAPI = require('../lib/fluxAPI');

async function testFluxAPI(){
  var ip = await fluxAPI.getMaster("38.242.243.3","33950");
  console.log(ip);
  ip = "::ffff:80.239.140.67";
  if(ip.includes(':')) ip = ip.split(':')[3];
  console.log(ip);

  ip = await fluxAPI.getMyIp("38.242.243.3","33950");
  console.log(ip);
}
testFluxAPI();