function convertIP(ip) {
  // eslint-disable-next-line no-param-reassign, prefer-destructuring
  if (ip.includes(':')) ip = ip.split(':')[3];
  return ip;
}
function htmlEscape(text) {
  return text.replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&rt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;')
    .replace(/\n/g, '</br>');
}

module.exports = {
  convertIP,
  htmlEscape,
};
