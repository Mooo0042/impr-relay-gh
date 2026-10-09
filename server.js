#!/usr/bin/env node
'use strict';

/**
 * IMPR relay server – drop-in replacement for the IamMusicPlayer upload relay.
 *
 * Contract (as implemented by the mod's UploadMusicMMMonitor):
 *   GET  /              -> {"Status":"Ok","Name","Version","MaxFileSize","Time":{"ResponseSpeed"}}
 *   POST /music-upload  -> raw MP3 bytes + header "mc-uuid"  ->  {"url": "..."}  or  {"Error","Message"}
 *
 * Extra: GET /f/<id>.mp3 serves the stored file (proxying Discord's expiring CDN links).
 * Needs Node 20+. No dependencies, except @vercel/blob when DB_STORE=blob or STORAGE=blob.
 */

const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');

const VERSION = '1.2.0';

const cfg = {
  port: Number(process.env.PORT || 3009),
  publicUrl: process.env.PUBLIC_URL ? process.env.PUBLIC_URL.replace(/\/*$/, '/') : '',
  name: process.env.RELAY_NAME || 'IMPR Relay',
  maxFileSize: Number(process.env.MAX_FILE_SIZE || 8 * 1024 * 1024),
  storage: (process.env.STORAGE || 'discord').toLowerCase(), // where the MP3s go
  dbStore: (process.env.DB_STORE || 'file').toLowerCase(), // where files.json lives: file | blob
  webhook: (process.env.DISCORD_WEBHOOK_URL || '').split('?')[0].replace(/\/+$/, ''),
  blobAccess: (process.env.BLOB_ACCESS || 'private').toLowerCase(), // must match the store's mode
  dataDir: path.resolve(process.env.DATA_DIR || './data'),
  rateLimit: Number(process.env.RATE_LIMIT_PER_HOUR || 20),
  retentionDays: Number(process.env.RETENTION_DAYS || 0),
  trustProxy: process.env.TRUST_PROXY === '1',
  allowedUuids: new Set(
    (process.env.ALLOWED_UUIDS || '')
      .split(',')
      .map((s) => s.trim().replace(/-/g, '').toLowerCase())
      .filter(Boolean)
  ),
};

const filesDir = path.join(cfg.dataDir, 'files');
const dbFile = path.join(cfg.dataDir, 'files.json');
const UUID_RE = /^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$/i;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(new Date().toISOString(), ...a);

/* ------------------------------------------------------------------ db */

// files.json maps file ids to where the MP3 lives. It is kept in memory and persisted either
// on local disk (DB_STORE=file) or in Vercel Blob (DB_STORE=blob), which makes the relay stateless.
const DB_BLOB_PATH = 'impr/files.json';
let db = { files: {}, hashes: {} };
let dbEtag = null; // blob only: ETag of the version we last read or wrote
let lastReload = 0;

const normalizeDb = (d) => ({ files: (d && d.files) || {}, hashes: (d && d.hashes) || {} });

// All db reads/writes run one at a time so a reload can never swap the db out mid-write.
let dbQueue = Promise.resolve();
function enqueue(fn) {
  const p = dbQueue.then(fn);
  dbQueue = p.catch(() => {});
  return p;
}

async function loadDb() {
  if (cfg.dbStore === 'blob') {
    const { get } = await sdk();
    const r = await get(DB_BLOB_PATH, { access: cfg.blobAccess, useCache: false });
    if (r && r.stream) {
      db = normalizeDb(await new Response(r.stream).json());
      dbEtag = (r.blob && r.blob.etag) || null;
    } else {
      db = normalizeDb(null); // first start, nothing stored yet
      dbEtag = null;
    }
    lastReload = Date.now();
    return;
  }
  try {
    db = normalizeDb(JSON.parse(await fsp.readFile(dbFile, 'utf8')));
  } catch (e) {
    if (e.code !== 'ENOENT') throw e; // never silently start with an empty db over a broken one
  }
}

async function writeDb() {
  const body = JSON.stringify(db); // snapshot before any await
  if (cfg.dbStore === 'blob') {
    const { put } = await sdk();
    const opts = { access: cfg.blobAccess, contentType: 'application/json', addRandomSuffix: false, cacheControlMaxAge: 60 };
    if (dbEtag) opts.ifMatch = dbEtag; // only overwrite the version we read
    else opts.allowOverwrite = false; // first write must not clobber an existing file
    const res = await put(DB_BLOB_PATH, body, opts);
    dbEtag = res.etag || null;
    return;
  }
  const tmp = dbFile + '.tmp';
  await fsp.writeFile(tmp, body);
  await fsp.rename(tmp, dbFile);
}

// Apply `fn` to the db and persist it. With Blob, a lost race (another instance wrote first)
// reloads the latest version and re-applies `fn` on top of it, so no entry is lost.
function mutateDb(fn) {
  return enqueue(async () => {
    for (let attempt = 0; ; attempt++) {
      fn(db);
      try {
        return await writeDb();
      } catch (e) {
        if (cfg.dbStore !== 'blob' || attempt >= 4) throw e;
        const { BlobPreconditionFailedError } = await sdk();
        if (!(e instanceof BlobPreconditionFailedError)) {
          if (dbEtag) throw e; // a real error, not a race
          await loadDb(); // first write failed: maybe someone created the file meanwhile
          if (!dbEtag) throw e;
        } else {
          await loadDb();
        }
      }
    }
  });
}

// Find a file entry; with a shared Blob db another instance may have added it since we last looked.
async function lookupFile(id) {
  if (db.files[id]) return db.files[id];
  if (cfg.dbStore === 'blob' && Date.now() - lastReload > 5000) {
    try {
      await enqueue(loadDb);
    } catch (e) {
      log('db reload failed:', e.message);
    }
  }
  return db.files[id];
}

/* ------------------------------------------------------ discord backend */

async function discordPut(id, buf) {
  const filename = `${id}.mp3`;
  const form = new FormData();
  form.append(
    'payload_json',
    JSON.stringify({ content: '', allowed_mentions: { parse: [] }, attachments: [{ id: 0, filename }] })
  );
  form.append('files[0]', new Blob([buf], { type: 'audio/mpeg' }), filename);

  for (let attempt = 0; attempt < 3; attempt++) {
    const res = await fetch(`${cfg.webhook}?wait=true`, { method: 'POST', body: form });
    if (res.status === 429) {
      const j = await res.json().catch(() => ({}));
      await sleep(Math.min((j.retry_after || 1) * 1000, 10000));
      continue;
    }
    if (!res.ok) throw new Error(`Discord returned HTTP ${res.status}`);
    const msg = await res.json();
    const att = msg.attachments && msg.attachments[0];
    if (!att) throw new Error('Discord response had no attachment');
    return { messageId: msg.id, url: att.url };
  }
  throw new Error('Discord rate limit, try again later');
}

// Discord CDN links are signed and expire (?ex=<hex unix time>). Fetching the
// message through the webhook returns freshly signed links.
const urlCache = new Map(); // id -> url

function expiryOf(u) {
  try {
    const ex = new URL(u).searchParams.get('ex');
    return ex ? parseInt(ex, 16) * 1000 : 0;
  } catch {
    return 0;
  }
}

async function discordUrl(meta, forceRefresh) {
  let url = urlCache.get(meta.id) || meta.url;
  const exp = expiryOf(url);
  const valid = exp === 0 || exp - Date.now() > 60_000;
  if (valid && !forceRefresh) return url;

  const res = await fetch(`${cfg.webhook}/messages/${meta.messageId}`);
  if (!res.ok) throw new Error(`Could not refresh Discord URL (HTTP ${res.status})`);
  const msg = await res.json();
  url = msg.attachments[0].url;
  urlCache.set(meta.id, url);
  return url;
}

async function discordStream(meta, req, res) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const url = await discordUrl(meta, attempt > 0);
    const headers = { 'accept-encoding': 'identity' };
    if (req.headers.range) headers.range = req.headers.range;
    const up = await fetch(url, { method: req.method === 'HEAD' ? 'HEAD' : 'GET', headers });

    if ((up.status === 403 || up.status === 404) && attempt === 0) {
      await up.body?.cancel().catch(() => {});
      continue; // link probably expired, refresh once
    }
    if (!up.ok) {
      await up.body?.cancel().catch(() => {});
      return json(res, 404, { Error: 'Not found', Message: 'File is no longer available' });
    }

    const out = { 'content-type': 'audio/mpeg', 'accept-ranges': 'bytes', 'cache-control': 'public, max-age=3600' };
    for (const h of ['content-length', 'content-range']) {
      const v = up.headers.get(h);
      if (v) out[h] = v;
    }
    res.writeHead(up.status, out);
    if (req.method === 'HEAD' || !up.body) return res.end();

    const body = Readable.fromWeb(up.body);
    res.on('close', () => body.destroy());
    return pipeline(body, res).catch(() => {});
  }
  return json(res, 502, { Error: 'Bad gateway', Message: 'Could not fetch file from Discord' });
}

async function discordRemove(meta) {
  await fetch(`${cfg.webhook}/messages/${meta.messageId}`, { method: 'DELETE' });
}

/* ------------------------------------------------------- local backend */

async function localPut(id, buf) {
  await fsp.writeFile(path.join(filesDir, `${id}.mp3`), buf);
  return {};
}

async function localStream(meta, req, res) {
  const file = path.join(filesDir, `${meta.id}.mp3`);
  let st;
  try {
    st = await fsp.stat(file);
  } catch {
    return json(res, 404, { Error: 'Not found', Message: 'File is no longer available' });
  }

  let start = 0;
  let end = st.size - 1;
  let status = 200;
  const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
  if (m && (m[1] || m[2])) {
    if (m[1] === '') {
      start = Math.max(0, st.size - Number(m[2]));
    } else {
      start = Number(m[1]);
      if (m[2]) end = Math.min(end, Number(m[2]));
    }
    if (start > end || start >= st.size) {
      res.writeHead(416, { 'content-range': `bytes */${st.size}` });
      return res.end();
    }
    status = 206;
  }

  const headers = {
    'content-type': 'audio/mpeg',
    'accept-ranges': 'bytes',
    'content-length': end - start + 1,
    'cache-control': 'public, max-age=3600',
  };
  if (status === 206) headers['content-range'] = `bytes ${start}-${end}/${st.size}`;
  res.writeHead(status, headers);
  if (req.method === 'HEAD') return res.end();
  return pipeline(fs.createReadStream(file, { start, end }), res).catch(() => {});
}

async function localRemove(meta) {
  await fsp.unlink(path.join(filesDir, `${meta.id}.mp3`)).catch(() => {});
}

/* -------------------------------------------------------- blob backend */

// Vercel Blob via the official SDK (the only dependency, loaded only for STORAGE=blob).
// Credentials are resolved by the SDK itself from the environment:
//   1. OIDC: BLOB_STORE_ID + VERCEL_OIDC_TOKEN (only exists on Vercel / after `vercel env pull`)
//   2. BLOB_READ_WRITE_TOKEN (works anywhere)
// BLOB_WEBHOOK_PUBLIC_KEY is only used for presigned *client* uploads; this relay uploads server-side.
let blobSdk;
async function sdk() {
  if (!blobSdk) {
    try {
      blobSdk = await import('@vercel/blob');
    } catch {
      throw new Error('STORAGE=blob needs the SDK: run "npm install"');
    }
  }
  return blobSdk;
}

async function blobPut(id, buf) {
  const { put } = await sdk();
  const blob = await put(`impr/${id}.mp3`, buf, {
    access: cfg.blobAccess,
    contentType: 'audio/mpeg',
    addRandomSuffix: false,
  });
  return { pathname: blob.pathname };
}

async function blobStream(meta, req, res) {
  const { get } = await sdk();
  const headers = {};
  if (req.headers.range) headers.range = req.headers.range;

  const r = await get(meta.pathname, { access: cfg.blobAccess, headers });
  if (!r || !r.stream) return fail(res, 404, 'Not found', 'File is no longer available');

  // The SDK reports every successful response as 200, so detect partial content by the header.
  const contentRange = r.headers.get('content-range');
  const out = { 'content-type': 'audio/mpeg', 'accept-ranges': 'bytes', 'cache-control': 'public, max-age=3600' };
  const len = r.headers.get('content-length');
  if (len) out['content-length'] = len;
  if (contentRange) out['content-range'] = contentRange;
  res.writeHead(contentRange ? 206 : 200, out);

  if (req.method === 'HEAD') {
    await r.stream.cancel().catch(() => {});
    return res.end();
  }
  const body = Readable.fromWeb(r.stream);
  res.on('close', () => body.destroy());
  return pipeline(body, res).catch(() => {});
}

async function blobRemove(meta) {
  const { del } = await sdk();
  await del(meta.pathname);
}

const storages = {
  discord: { put: discordPut, stream: discordStream, remove: discordRemove },
  local: { put: localPut, stream: localStream, remove: localRemove },
  blob: { put: blobPut, stream: blobStream, remove: blobRemove },
};
const storage = storages[cfg.storage];

/* ------------------------------------------------------------- helpers */

function json(res, status, obj, extra = {}) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body),
    ...extra,
  });
  res.end(body);
}

const fail = (res, status, error, message, extra) => json(res, status, { Error: error, Message: message }, extra);

function clientIp(req) {
  if (cfg.trustProxy) {
    const xff = req.headers['x-forwarded-for'];
    if (xff) return String(xff).split(',')[0].trim();
  }
  return req.socket.remoteAddress || 'unknown';
}

function baseUrl(req) {
  if (cfg.publicUrl) return cfg.publicUrl;
  const proto = req.headers['x-forwarded-proto'] || (process.env.VERCEL ? 'https' : 'http');
  return `${proto}://${req.headers.host}/`;
}

const hits = new Map();
function rateLimited(key, max) {
  const now = Date.now();
  const arr = (hits.get(key) || []).filter((t) => now - t < 3_600_000);
  if (arr.length >= max) {
    hits.set(key, arr);
    return true;
  }
  arr.push(now);
  hits.set(key, arr);
  return false;
}

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let n = 0;
    let tooLarge = false;
    req.on('data', (c) => {
      n += c.length;
      if (n > limit) {
        tooLarge = true;
        chunks.length = 0; // stop storing, keep draining
        if (n > limit * 4) req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(tooLarge ? null : Buffer.concat(chunks)));
    req.on('error', reject);
    req.on('close', () => resolve(null));
  });
}

function looksLikeMp3(b) {
  if (b.length < 128) return false;
  if (b.toString('latin1', 0, 3) === 'ID3') return true;
  return b[0] === 0xff && (b[1] & 0xe0) === 0xe0; // MPEG frame sync
}

/* ------------------------------------------------------------ handlers */

function handleStatus(req, res) {
  const t0 = Date.now();
  json(res, 200, {
    Status: 'Ok',
    Name: cfg.name,
    Version: VERSION,
    MaxFileSize: cfg.maxFileSize,
    Time: { ResponseSpeed: Date.now() - t0 },
  });
}

async function handleUpload(req, res) {
  const uuid = String(req.headers['mc-uuid'] || '').toLowerCase();
  if (!UUID_RE.test(uuid)) return fail(res, 400, 'Bad request', 'Missing or invalid mc-uuid header');
  if (cfg.allowedUuids.size && !cfg.allowedUuids.has(uuid.replace(/-/g, ''))) {
    return fail(res, 403, 'Forbidden', 'This relay is private');
  }
  if (rateLimited(`u:${uuid}`, cfg.rateLimit) || rateLimited(`ip:${clientIp(req)}`, cfg.rateLimit * 5)) {
    return fail(res, 429, 'Too many uploads', 'Please try again later');
  }

  const declared = Number(req.headers['content-length']);
  if (declared > cfg.maxFileSize) {
    return fail(res, 413, 'File too large', `Maximum size is ${cfg.maxFileSize} bytes`, { connection: 'close' });
  }

  const buf = await readBody(req, cfg.maxFileSize);
  if (!buf) return fail(res, 413, 'File too large', `Maximum size is ${cfg.maxFileSize} bytes`);
  if (buf.length === 0) return fail(res, 400, 'Bad request', 'Empty body');
  if (!looksLikeMp3(buf)) return fail(res, 415, 'Unsupported file', 'Only MP3 files are accepted');

  const hash = crypto.createHash('sha256').update(buf).digest('hex');
  let id = db.hashes[hash];
  if (!id || !db.files[id]) {
    id = crypto.randomBytes(9).toString('base64url');
    const fileId = id;
    try {
      const meta = await storage.put(fileId, buf);
      const entry = { id: fileId, ...meta, size: buf.length, created: Date.now(), uuid };
      await mutateDb((d) => {
        d.files[fileId] = entry;
        d.hashes[hash] = fileId;
      });
    } catch (e) {
      log('upload failed:', e.message);
      return fail(res, 502, 'Storage error', 'There was a problem processing the upload, please try again later');
    }
    log(`stored ${id} (${buf.length} bytes) from ${uuid}`);
  }
  json(res, 200, { url: `${baseUrl(req)}f/${id}.mp3` });
}

async function handleFile(req, res, id) {
  const meta = await lookupFile(id);
  if (!meta) return fail(res, 404, 'Not found', 'Unknown file');
  return storage.stream(meta, req, res);
}

const handleRequest = async (req, res) => {
  try {
    const url = new URL(req.url, 'http://x');
    const p = url.pathname;
    const get = req.method === 'GET' || req.method === 'HEAD';

    if (get && (p === '/' || p === '/status')) return handleStatus(req, res);
    if (req.method === 'POST' && p === '/music-upload') return await handleUpload(req, res);
    const m = /^\/f\/([A-Za-z0-9_-]{6,32})(?:\.mp3)?$/.exec(p);
    if (get && m) return await handleFile(req, res, m[1]);
    fail(res, 404, 'Not found', 'Unknown route');
  } catch (e) {
    log('error:', e);
    if (!res.headersSent) fail(res, 500, 'Internal error', 'Unexpected server error');
    else res.destroy();
  }
};

async function cleanup() {
  const now = Date.now();
  for (const [k, arr] of hits) if (!arr.some((t) => now - t < 3_600_000)) hits.delete(k);
  if (!cfg.retentionDays) return;

  const cutoff = now - cfg.retentionDays * 86_400_000;
  const stale = Object.values(db.files).filter((m) => m.created < cutoff);
  for (const meta of stale) {
    try {
      await storage.remove(meta);
    } catch (e) {
      log('remove failed:', meta.id, e.message);
    }
    urlCache.delete(meta.id);
  }
  if (stale.length) {
    await mutateDb((d) => {
      for (const meta of stale) {
        delete d.files[meta.id];
        for (const [h, hid] of Object.entries(d.hashes)) if (hid === meta.id) delete d.hashes[h];
      }
    });
    log(`retention: removed ${stale.length} file(s)`);
  }
}

async function initialize() {
  if (!storage) throw new Error(`Unknown STORAGE "${cfg.storage}" (use "discord", "blob" or "local")`);
  if (!['file', 'blob'].includes(cfg.dbStore)) throw new Error(`Unknown DB_STORE "${cfg.dbStore}" (use "file" or "blob")`);
  if (cfg.storage === 'discord' && !/^https:\/\/(\w+\.)?discord(app)?\.com\/api\/webhooks\//.test(cfg.webhook)) {
    throw new Error('STORAGE=discord needs a valid DISCORD_WEBHOOK_URL');
  }

  const usesBlob = cfg.storage === 'blob' || cfg.dbStore === 'blob';
  if (usesBlob) {
    const why = cfg.dbStore === 'blob' ? 'DB_STORE=blob' : 'STORAGE=blob';
    if (!['private', 'public'].includes(cfg.blobAccess)) throw new Error('BLOB_ACCESS must be "private" or "public" (it has to match your Blob store)');
    const hasToken = !!process.env.BLOB_READ_WRITE_TOKEN;
    const hasOidc = !!(process.env.BLOB_STORE_ID && process.env.VERCEL_OIDC_TOKEN);
    if (!hasToken && !hasOidc) throw new Error(`${why} needs BLOB_READ_WRITE_TOKEN (or BLOB_STORE_ID + VERCEL_OIDC_TOKEN when running on Vercel).`);
    await sdk();
    if (cfg.dbStore === 'blob' && cfg.blobAccess === 'public') {
      log('WARNING: DB_STORE=blob with a public store makes files.json (player UUIDs) readable by anyone with the URL. Use a private store.');
    }
  }

  if (cfg.storage === 'local' || cfg.dbStore === 'file') await fsp.mkdir(filesDir, { recursive: true });
  await loadDb();
  if (!process.env.VERCEL) setInterval(() => cleanup().catch((e) => log('cleanup error:', e)), 3_600_000).unref();
  log(`${cfg.name} v${VERSION} initialized (files=${cfg.storage}, db=${cfg.dbStore}, ${Object.keys(db.files).length} known, max=${cfg.maxFileSize} bytes)`);
}

const initialized = initialize();

async function handler(req, res) {
  try {
    await initialized;
    await handleRequest(req, res);
  } catch (e) {
    log('initialization/request error:', e && e.stack || e);
    if (!res.headersSent) fail(res, 500, 'Relay initialization error', String(e && e.message || e));
    else res.destroy();
  }
}

module.exports = handler;

if (require.main === module) {
  initialized.then(() => {
    http.createServer(handler).listen(cfg.port, () => log(`${cfg.name} v${VERSION} listening on :${cfg.port}`));
  }).catch((e) => {
    console.error('Relay startup failed:', e && e.stack || e);
    process.exitCode = 1;
  });
}
