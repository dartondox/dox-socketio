// A Node.js Socket.IO server with @socket.io/redis-adapter, next to a Dart
// server on the same Redis. Checks the requests that go to all servers.
// Usage: node redis-cluster.mjs <redisUrl> <msgpack|json> <key>
//
// It prints READY, then answers the Dart server:
// - `ping-node` (serverSideEmit with an ack): answers `pong from node: <arg>`.
// - `note` (serverSideEmit): prints `NOTE <arg>`.
// - its client answers `ask` with `node`.
// - when its socket joins `from-dart`, it prints `JOINED from-dart`.
// On `GO` from stdin it asks the Dart server, prints DONE and exits 0.
import { createServer } from 'node:http';
import { createInterface } from 'node:readline';
import { Server } from 'socket.io';
import { io as connect } from 'socket.io-client';
import { createAdapter } from '@socket.io/redis-adapter';
import { createClient } from 'redis';

const [redisUrl, format, key] = process.argv.slice(2);
const timer = setTimeout(() => { console.error('FAIL timed out'); process.exit(1); }, 30000);
const fail = (text) => { console.error('FAIL ' + text); process.exit(1); };

const pub = createClient({ url: redisUrl });
const sub = pub.duplicate();
await Promise.all([pub.connect(), sub.connect()]);

const parser = format === 'json'
  ? { encode: (value) => JSON.stringify(value), decode: (value) => JSON.parse(value) }
  : undefined;
const http = createServer();
const io = new Server(http, { adapter: createAdapter(pub, sub, { key, parser }) });
io.on('connection', (socket) => {
  socket.data = { from: 'node' };
  socket.join('r');
});
io.on('ping-node', (arg, callback) => callback('pong from node: ' + arg));
io.on('note', (arg) => console.log('NOTE ' + arg));
io.of('/').adapter.on('join-room', (room) => {
  if (room === 'from-dart') console.log('JOINED from-dart');
});
await new Promise((resolve) => http.listen(0, '127.0.0.1', resolve));

const client = connect(`http://127.0.0.1:${http.address().port}`, { transports: ['websocket'] });
client.on('ask', (callback) => callback('node'));
client.on('connect', () => setTimeout(() => console.log('READY'), 300));

async function askDart() {
  const sockets = await io.fetchSockets();
  if (sockets.length !== 2) fail('fetchSockets gave ' + sockets.length);
  const dart = sockets.find((socket) => socket.data.from === 'dart');
  if (!dart) fail('no Dart socket in ' + JSON.stringify(sockets.map((s) => s.data)));
  if (!dart.rooms.has('r')) fail('Dart socket rooms ' + [...dart.rooms]);
  const answers = await io.serverSideEmitWithAck('ping-dart', 'y');
  if (JSON.stringify(answers) !== '["pong from dart: y"]') fail('answers ' + JSON.stringify(answers));
  io.serverSideEmit('note-dart', 'hey');
  const acks = await io.timeout(3000).emitWithAck('ask');
  if (JSON.stringify(acks.sort()) !== '["dart","node"]') fail('acks ' + JSON.stringify(acks));
  console.log('DONE');
  clearTimeout(timer);
  client.close();
  io.close();
  pub.quit().catch(() => {});
  sub.quit().catch(() => {});
  setTimeout(() => process.exit(0), 100);
}

createInterface({ input: process.stdin }).on('line', (line) => {
  if (line === 'GO') askDart().catch((error) => fail(error.stack));
});
