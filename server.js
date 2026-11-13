'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { spawn } = require('child_process');

const cfg = {
  port: +process.env.PORT || 3000,
  host: process.env.HOST || '0.0.0.0',
  jar: path.resolve(process.env.RARS_JAR || path.join(__dirname, 'rars1_6.jar')),
  java: process.env.JAVA_BIN || 'java',
  timeoutMs: +process.env.TIMEOUT_MS || 60000,
  maxOut: +process.env.MAX_OUTPUT || 2 * 1024 * 1024,
  maxSrc: 1024 * 1024,
  maxSessions: 8,
};

if (!fs.existsSync(cfg.jar)) {
  console.error(`RARS jar not found at ${cfg.jar}\nPut rars1_6.jar next to server.js or set RARS_JAR.`);
  process.exit(1);
}

const INDEX = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const sessions = new Map();

function send(res, ev) { res.write(`data: ${JSON.stringify(ev)}\n\n`); }

function emit(s, t, d) {
  const ev = { t, d };
  s.events.push(ev);
  for (const r of s.clients) send(r, ev);
}

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => {
      size += c.length;
      if (size > limit) { reject(new Error('Body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function startRun(name, source) {
  const id = crypto.randomBytes(8).toString('hex');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rars-'));
  const safe = (path.basename(name).replace(/[^\w.-]/g, '_') || 'prog.s');
  const file = path.join(dir, safe.endsWith('.s') || safe.endsWith('.asm') ? safe : safe + '.s');
  fs.writeFileSync(file, source);

  const s = { id, events: [], clients: new Set(), done: false, child: null, outBytes: 0 };
  sessions.set(id, s);

  const child = spawn(cfg.java, ['-Djava.awt.headless=true', '-jar', cfg.jar, 'nc', file], { cwd: dir });
  s.child = child;
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdin.on('error', () => {});

  const timer = setTimeout(() => {
    emit(s, 'err', `\n[Timed out after ${cfg.timeoutMs / 1000}s, killing]\n`);
    child.kill('SIGKILL');
  }, cfg.timeoutMs);

  const onData = kind => d => {
    s.outBytes += d.length;
    if (s.outBytes > cfg.maxOut) {
      if (!s.truncated) { s.truncated = true; emit(s, 'err', '\n[Output limit hit, killing]\n'); child.kill('SIGKILL'); }
      return;
    }
    emit(s, kind, d);
  };
  child.stdout.on('data', onData('out'));
  child.stderr.on('data', onData('err'));

  child.on('error', e => emit(s, 'err', `[Failed to start "${cfg.java}": ${e.message}]\n`));
  child.on('close', (code, signal) => {
    clearTimeout(timer);
    s.done = true;
    emit(s, 'exit', code ?? signal);
    for (const r of s.clients) r.end();
    s.clients.clear();
    fs.rm(dir, { recursive: true, force: true }, () => {});
    setTimeout(() => sessions.delete(id), 60000).unref();
  });
  return id;
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://x');
    const parts = url.pathname.split('/').filter(Boolean);

    if (req.method === 'GET' && url.pathname === '/') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'X-Content-Type-Options': 'nosniff' });
      return res.end(INDEX);
    }

    if (req.method === 'POST' && parts[0] === 'run') {
      if ([...sessions.values()].filter(s => !s.done).length >= cfg.maxSessions) {
        res.writeHead(429); return res.end('Too many running programs');
      }
      const body = await readBody(req, cfg.maxSrc);
      const id = startRun(url.searchParams.get('name') || 'prog.s', body);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ id }));
    }

    const s = sessions.get(parts[1]);
    if (parts[0] === 'stream' && req.method === 'GET') {
      if (!s) { res.writeHead(404); return res.end('No such session'); }
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
      for (const ev of s.events) send(res, ev);
      if (s.done) return res.end();
      s.clients.add(res);
      req.on('close', () => s.clients.delete(res));
      return;
    }
    if (parts[0] === 'stdin' && req.method === 'POST') {
      if (!s || s.done) { res.writeHead(404); return res.end('Not running'); }
      s.child.stdin.write(await readBody(req, 64 * 1024));
      res.writeHead(204); return res.end();
    }
    if (parts[0] === 'kill' && req.method === 'POST') {
      if (s && !s.done) s.child.kill('SIGKILL');
      res.writeHead(204); return res.end();
    }

    res.writeHead(404); res.end('Not found');
  } catch (e) {
    if (!res.headersSent) res.writeHead(400);
    res.end(String(e.message));
  }
});

server.listen(cfg.port, cfg.host, () => {
  console.log(`RARS console on http://${cfg.host}:${cfg.port}  (jar: ${cfg.jar})`);
});

function shutdown() {
  for (const s of sessions.values()) if (!s.done) s.child.kill('SIGKILL');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
