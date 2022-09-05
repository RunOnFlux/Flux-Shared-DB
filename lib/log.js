/* eslint no-console: ["error", { allow: ["warn", "error", "log"] }] */
const fs = require('fs');
const config = require('../ClusterOperator/config');

function ensureString(parameter) {
  return parameter.toString();
}

function getFilesizeInBytes(filename) {
  try {
    const stats = fs.statSync(filename);
    const fileSizeInBytes = stats.size;
    return fileSizeInBytes;
  } catch (e) {
    console.log(e);
    return 0;
  }
}
function writeToFile(args) {
  const size = getFilesizeInBytes('logs.txt');
  let flag = 'a+';
  if (size > (25 * 1024 * 1024)) { // 25MB
    flag = 'w'; // rewrite file
  }
  const stream = fs.createWriteStream('logs.txt', { flags: flag });
  stream.write(`${new Date().toISOString()}          ${ensureString(args.message || args)}\n`);
  if (args.stack && typeof args.stack === 'string') {
    stream.write(`${args.stack}\n`);
  }
  stream.end();
}
module.exports = {
  error(...args) {
    if (config.debugMode) console.error(...args);
    writeToFile(args);
  },

  warn(...args) {
    if (config.debugMode) console.warn(...args);
    writeToFile(args);
  },

  info(...args) {
    if (config.debugMode) console.log(...args);
    writeToFile(args);
  },

  debug(...args) {
    if (config.debugMode) console.log(...args);
    writeToFile(args);
  },
};
