#!/usr/bin/env node
'use strict';
/* Servidor multijugador per a DOORS 8-BIT.
   Sense dependències: HTTP + WebSocket implementat a mà.
   Ús:  node server.js   (port per defecte 3000, canvia'l amb PORT=8080 node server.js) */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');

const PORT = +process.env.PORT || 3000;
const MAX_PLAYERS = 8;
const MAX_PARTIES = 200;
const MAX_MSG = 4096;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const COLORS = ['#e84a4a', '#4aa8e8', '#6ad65a', '#e8d34a', '#c36ae8', '#e88a3c', '#4ae8c8', '#e86ab0'];
const KEY_RE = /^[a-z]{1,2}\d{1,3}(:\d{1,3})?$/;

const parties = new Map();
let nextId = 1;

const newSeed = () => crypto.randomInt(1, 2 ** 31 - 1);

/* ---------- HTTP (serveix index.html) ---------- */
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) { res.writeHead(500); return res.end('Falta index.html al costat de server.js'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('No trobat');
  }
});

/* ---------- WebSocket ---------- */
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || !req.url.startsWith('/ws')) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { id: 0, socket, buf: Buffer.alloc(0), frag: [], party: null, name: '', color: '#fff', dead: false, done: false };
  socket.on('data', chunk => onData(c, chunk));
  socket.on('close', () => drop(c));
  socket.on('error', () => socket.destroy());
});

function frame(op, payload) {
  const len = payload.length;
  let h;
  if (len < 126) h = Buffer.from([0x80 | op, len]);
  else if (len < 65536) h = Buffer.from([0x80 | op, 126, len >> 8, len & 255]);
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([h, payload]);
}
function wsSend(c, obj) {
  if (!c.socket.writable) return;
  c.socket.write(frame(1, Buffer.from(JSON.stringify(obj))));
}
function closeClient(c) {
  try { c.socket.write(frame(8, Buffer.alloc(0))); } catch (e) { /* res */ }
  c.socket.end();
}

function onData(c, chunk) {
  c.buf = Buffer.concat([c.buf, chunk]);
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_MSG) { closeClient(c); return; }
    const mlen = masked ? 4 : 0;
    if (b.length < off + mlen + len) return;
    let payload = b.subarray(off + mlen, off + mlen + len);
    if (masked) {
      const m = b.subarray(off, off + 4), out = Buffer.alloc(len);
      for (let i = 0; i < len; i++) out[i] = payload[i] ^ m[i & 3];
      payload = out;
    }
    c.buf = b.subarray(off + mlen + len);
    if (op === 8) { closeClient(c); return; }
    if (op === 9) { c.socket.write(frame(10, payload)); continue; }
    if (op === 1 || op === 0) {
      c.frag.push(payload);
      if (fin) {
        const text = Buffer.concat(c.frag).toString('utf8');
        c.frag = [];
        let m = null;
        try { m = JSON.parse(text); } catch (e) { /* ignora */ }
        if (m && typeof m === 'object') onMsg(c, m);
      }
    }
  }
}

/* ---------- Lògica de sales ---------- */
function broadcast(party, obj, except) {
  for (const p of party.players.values()) if (p !== except) wsSend(p, obj);
}
function checkDone(party) {
  const ps = [...party.players.values()];
  if (ps.length && ps.every(p => p.dead || p.done)) {
    party.seed = newSeed();
    party.state = {};
    for (const p of ps) { p.dead = false; p.done = false; }
    broadcast(party, { t: 'reset', seed: party.seed });
  }
}
function onMsg(c, m) {
  if (!c.party) {
    if (m.t !== 'join') return;
    const code = String(m.room || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8) || 'SALA';
    let party = parties.get(code);
    if (!party) {
      if (parties.size >= MAX_PARTIES) { wsSend(c, { t: 'err', msg: 'Servidor ple' }); closeClient(c); return; }
      party = { code, seed: newSeed(), players: new Map(), state: {}, timer: null };
      parties.set(code, party);
    }
    if (party.players.size >= MAX_PLAYERS) { wsSend(c, { t: 'err', msg: 'Sala plena (màxim ' + MAX_PLAYERS + ')' }); closeClient(c); return; }
    if (party.timer) { clearTimeout(party.timer); party.timer = null; }
    const used = new Set([...party.players.values()].map(p => p.color));
    c.id = nextId++;
    c.color = COLORS.find(x => !used.has(x)) || COLORS[c.id % COLORS.length];
    c.name = String(m.name || 'Jugador').replace(/[^\w \-\u00C0-\u017F]/g, '').slice(0, 12) || 'Jugador';
    c.party = party;
    wsSend(c, {
      t: 'welcome', id: c.id, color: c.color, seed: party.seed, state: party.state,
      players: [...party.players.values()].map(p => ({ id: p.id, name: p.name, color: p.color }))
    });
    party.players.set(c.id, c);
    broadcast(party, { t: 'join', id: c.id, name: c.name, color: c.color }, c);
    console.log('[' + code + '] ' + c.name + ' entra (' + party.players.size + ' jugadors)');
    return;
  }
  const party = c.party;
  switch (m.t) {
    case 'p':
      broadcast(party, { t: 'p', id: c.id, r: m.r | 0, x: +m.x || 0, y: +m.y || 0,
        d: String(m.d || 'd').slice(0, 1), h: m.h ? 1 : 0, z: m.z ? 1 : 0 }, c);
      break;
    case 'set':
      if (typeof m.k === 'string' && KEY_RE.test(m.k) && (typeof m.v === 'number' || typeof m.v === 'string')
          && Object.keys(party.state).length < 2000) {
        party.state[m.k] = m.v;
        broadcast(party, { t: 'set', k: m.k, v: m.v }, c);
      }
      break;
    case 'ev':
      if (m.k === 'rush') broadcast(party, { t: 'ev', k: 'rush', r: m.r | 0 }, c);
      break;
    case 'toast':
      if (typeof m.text === 'string') broadcast(party, { t: 'toast', text: m.text.slice(0, 60) }, c);
      break;
    case 'dead': c.dead = true; checkDone(party); break;
    case 'esc': c.done = true; checkDone(party); break;
  }
}
function drop(c) {
  const party = c.party;
  if (!party) return;
  c.party = null;
  party.players.delete(c.id);
  broadcast(party, { t: 'leave', id: c.id });
  console.log('[' + party.code + '] ' + c.name + ' surt (' + party.players.size + ' jugadors)');
  if (!party.players.size) {
    party.timer = setTimeout(() => { if (!party.players.size) parties.delete(party.code); }, 60000);
  } else checkDone(party);
}

/* manté les connexions vives */
setInterval(() => {
  for (const party of parties.values())
    for (const c of party.players.values()) if (c.socket.writable) c.socket.write(frame(9, Buffer.alloc(0)));
}, 25000).unref();

server.listen(PORT, () => {
  console.log('DOORS 8-BIT en marxa. Obre:');
  console.log('  http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list || [])
      if (i.family === 'IPv4' && !i.internal) console.log('  http://' + i.address + ':' + PORT + '   (per a jugadors de la teva xarxa)');
});
