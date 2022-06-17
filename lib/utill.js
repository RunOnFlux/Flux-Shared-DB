function convertIP(ip){
  if(ip.includes(':')) ip = ip.split(':')[3];
  return ip;
}
module.exports = {
  convertIP,

};