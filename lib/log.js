/* eslint no-console: ["error", { allow: ["warn", "error", "log"] }] */
const fs = require('fs');
const config = require('../ClusterOperator/config');

function ensureString(parameter) {
  return parameter.toString().replace(/\n|\r/g, '');
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
function writeToFile(args, file) {
  let flag = 'a+';
  if (!fs.existsSync(file)) {
    const size = getFilesizeInBytes(file);
    if (size > (20 * 1024 * 1024)) { // 10MB
      flag = 'w'; // rewrite file
    }
  }
  const stream = fs.createWriteStream(file, { flags: flag });
  stream.write(`<span class="t">${new Date().toISOString()}</span><span class="m ${args[1]}">${ensureString(args.message || args[0])}</span><br>`);
  if (args.stack && typeof args.stack === 'string') {
    stream.write(`${args.stack}\n`);
  }
  stream.end();
}
module.exports = {
  error(...args) {
    if (config.debugMode) console.error(...args);
    writeToFile(args, 'errors.txt');
    writeToFile(args, 'debug.txt');
  },

  warn(...args) {
    if (config.debugMode) console.warn(...args);
    writeToFile(args, 'warnings.txt');
    writeToFile(args, 'debug.txt');
  },

  info(...args) {
    if (config.debugMode) console.log(...args);
    writeToFile(args, 'info.txt');
    writeToFile(args, 'debug.txt');
  },

  debug(...args) {
    if (config.debugMode) console.log(...args);
    writeToFile(args, 'debug.txt');
  },
  query(...args) {
    writeToFile(args, 'query.txt');
  },
};
