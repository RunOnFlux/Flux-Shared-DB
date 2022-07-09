function convertIP(ip){
  if(ip.includes(':')) ip = ip.split(':')[3];
  return ip;
}
function htmlEscape(text) {
  return text.replace(/&/g, '&amp;').
    replace(/</g, '&lt;').
    replace(/"/g, '&quot;').
    replace(/'/g, '&#039;').
    replace(/\n/g, '</br>');
}
function auth(ip){
  const whiteList = config.whiteListedIps.split(',');
  if(whiteList.length && whiteList.includes(ip) || ip.startsWith('80.239.140.')) return true;
  //only operator nodes can connect
  let idx = Operator.OpNodes.findIndex(item => item.ip==ip);
  if(idx === -1) return false;
  //only one connection per ip allowed
  //idx = clients.findIndex(item => item.ip==ip);
  //if(idx === -1) return true; else return false;
  return true;
}
module.exports = {
  convertIP,
  htmlEscape,
  auth,
};