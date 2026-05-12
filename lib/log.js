/* eslint no-console: ["error", { allow: ["warn", "error", "log"] }] */
const fs = require('fs');
const config = require('../ClusterOperator/config');

function ensureString(parameter) {
  return parameter.toString()
    .replace(/\n|\r/g, '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Per-file write queues: each file has a serialized promise chain so concurrent
// log calls never race (no concurrent fs.stat + fs.writeFile wipeouts).
const writeQueues = {};

function writeToFile(args, file) {
  const line = `<span class="t">${new Date().toISOString()}</span><span class="m ${args[1]}">${ensureString(args.message || args[0])}</span><br>${args.stack && typeof args.stack === 'string' ? `${args.stack}\n` : ''}`;
  if (!writeQueues[file]) writeQueues[file] = Promise.resolve();
  writeQueues[file] = writeQueues[file].then(() => new Promise((resolve) => {
    fs.stat(file, (statErr, stats) => {
      if (!statErr && stats.size > (20 * 1024 * 1024)) { // 20MB — truncate, then write
        fs.writeFile(file, line, (err) => {
          if (err) console.error('Log write error:', err);
          resolve();
        });
      } else {
        fs.appendFile(file, line, (err) => {
          if (err) console.error('Log append error:', err);
          resolve();
        });
      }
    });
  }));
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
  compress(...args) {
    writeToFile(args, 'compress.txt');
    writeToFile(args, 'debug.txt');
  },
};
