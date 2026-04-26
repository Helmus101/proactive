const fs = require('fs');
const fsPromises = fs.promises;

async function existsAsync(path) {
  try {
    await fsPromises.access(path);
    return true;
  } catch {
    return false;
  }
}

function withTimeout(promise, timeoutMs, label = 'operation') {
  let timer = null;
  return Promise.race([
    Promise.resolve(promise),
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
    })
  ]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

module.exports = {
  existsAsync,
  withTimeout
};
