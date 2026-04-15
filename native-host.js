const fs = require('fs');

/**
 * Native Messaging Host for Chrome.
 * Implements the 32-bit length-prefixed protocol and relays messages
 * between the Chrome extension and the running desktop app.
 */

function readMessage() {
  const lengthBuffer = Buffer.alloc(4);
  const bytesRead = fs.readSync(0, lengthBuffer, 0, 4);
  if (bytesRead === 0) process.exit(0);
  
  const length = lengthBuffer.readUInt32LE(0);
  const messageBuffer = Buffer.alloc(length);
  fs.readSync(0, messageBuffer, 0, length);
  
  return JSON.parse(messageBuffer.toString());
}

function sendMessage(msg) {
  const buffer = Buffer.from(JSON.stringify(msg));
  const lengthBuffer = Buffer.alloc(4);
  lengthBuffer.writeUInt32LE(buffer.length, 0);
  
  process.stdout.write(lengthBuffer);
  process.stdout.write(buffer);
}

const WebSocket = require('ws');
let ws = null;
const pendingForApp = [];

function connectToApp() {
  ws = new WebSocket('ws://localhost:3003');
  
  ws.on('open', () => {
    queueForApp({
      type: "HOST_STATUS",
      role: "native-host",
      status: "connected_to_app",
      timestamp: Date.now()
    });
    flushPendingForApp();
  });

  ws.on('message', (data) => {
    const msg = JSON.parse(data.toString());
    if (msg.type === "EXECUTE_ACTION" || msg.type === "run-diagnostic" || msg.type === "execute-task" || msg.type === "APP_BRIDGE_PING") {
      sendMessage(msg);
    }
  });

  ws.on('close', () => {
    ws = null;
    setTimeout(connectToApp, 2000);
  });

  ws.on('error', () => {
    // App might not be running yet
  });
}

function queueForApp(msg) {
  pendingForApp.push(msg);
}

function flushPendingForApp() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  while (pendingForApp.length) {
    ws.send(JSON.stringify(pendingForApp.shift()));
  }
}

connectToApp();

// Read from stdin (Chrome) and forward to WebSocket (App)
process.stdin.on('readable', () => {
  try {
    const msg = readMessage();
    if (msg) {
      queueForApp(msg);
      flushPendingForApp();
    }
  } catch (e) {
    // silence errors for now
  }
});

process.on('uncaughtException', (err) => {
  // Prevent crash
});
