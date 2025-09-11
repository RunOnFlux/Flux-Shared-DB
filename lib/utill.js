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
function trimArrayToSize(array, maxSize) {
  const newArray = [];
  let currentSize = 0;

  // eslint-disable-next-line no-restricted-syntax
  for (const item of array) {
    const serializedItem = JSON.stringify(item); // Serialize the item to estimate its size in bytes
    const itemSize = new TextEncoder().encode(serializedItem).length;

    if (currentSize + itemSize <= maxSize) {
      newArray.push(item);
      currentSize += itemSize;
    } else {
      if (newArray.length === 0) newArray.push(item);
      break; // Stop adding items if the size limit is exceeded
    }
  }

  return newArray;
}

module.exports = {
  convertIP,
  htmlEscape,
  trimArrayToSize,
};
