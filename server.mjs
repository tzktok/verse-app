import http from 'node:http';
import { pathToFileURL } from 'node:url';
import { OAuth2Client } from 'google-auth-library';
import { Store, createPool, migrate, SONG_COST, InsufficientCredits } from './store.mjs';
import { createAudioStorage } from './audio-storage.mjs';

export function validateInput(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new Error('Invalid song request.');
  const { lyrics = '', prompt = '', is_instrumental = false, lyrics_optimizer = false } = body;
  if (typeof lyrics !== 'string' || lyrics.length > 3500) throw new Error('Lyrics must be text with at most 3,500 characters.');
  if (typeof prompt !== 'string' || prompt.length > 2000) throw new Error('Sound direction must be text with at most 2,000 characters.');
  if (typeof is_instrumental !== 'boolean' || typeof lyrics_optimizer !== 'boolean') throw new Error('Invalid music options.');
  if (!is_instrumental && !lyrics_optimizer && !lyrics.trim()) throw new Error('Add lyrics to create a vocal track.');
  if ((is_instrumental || lyrics_optimizer) && !prompt.trim()) throw new Error('Describe the sound you want to create.');
  return { lyrics, prompt, is_instrumental, lyrics_optimizer, audio_format: 'mp3', sample_rate: 44100, bitrate: 256000 };
}
export function audioUrl(output) {
  if (typeof output === 'string' && output.startsWith('https://')) return output;
  if (Array.isArray(output)) return output.map(audioUrl).find(Boolean) ?? null;
  if (output && typeof output === 'object') return audioUrl(output.audio ?? output.url ?? output.audio_file);
  return null;
}
export async function verifyGoogleIdToken(idToken, audience = process.env.GOOGLE_WEB_CLIENT_ID) {
  if (!audience) throw new Error('GOOGLE_WEB_CLIENT_ID is required.');
  const ticket = await new OAuth2Client(audience).verifyIdToken({ idToken, audience });
  const payload = ticket.getPayload();
  if (!payload?.sub || !payload.email || !payload.email_verified) throw new Error('Google account has no verified email.');
  return { sub: payload.sub, email: payload.email, name: payload.name || payload.email.split('@')[0] };
}
async function readJson(req, maxBytes = 24000) {
  if (!(req.headers['content-type'] || '').startsWith('application/json')) throw new Error('Content-Type must be application/json.');
  const chunks = []; let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > maxBytes) throw new Error('Request is too large.');
    chunks.push(chunk);
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
export function publicSong(row) {
  return { id: row.id, title: row.title, style: row.style, status: row.status,
    audio_url: row.audio_url, error: row.error, created_at: row.created_at, settings: row.settings ?? {}, stored: Boolean(row.storage_key) };
}
async function presentSong(row, storage) {
  const result = publicSong(row);
  if (row.storage_key) result.audio_url = storage ? await storage.url(row.storage_key) : null;
  return result;
}
async function finishPrediction(store, song, data, storage) {
  const source = audioUrl(data.output);
  if (data.status === 'succeeded' && source && storage) {
    await store.markSaving(song.id, source);
    return store.getSong(song.user_id, song.id);
  }
  return store.settleSong(song.id, data.status, source, data.error ? 'Generation failed. Your 100 credits have been returned.' : null);
}
async function syncPrediction(store, song, token, fetchImpl, storage, persist = false) {
  if (song.status === 'saving') return storage && persist ? store.saveAudio(song.id, storage) : song;
  if (!song.replicate_id || !['starting','processing'].includes(song.status)) return song;
  const upstream = await fetchImpl(`https://api.replicate.com/v1/predictions/${song.replicate_id}`, {
    headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(35000),
  });
  if (!upstream.ok) return song;
  const data = await upstream.json();
  return finishPrediction(store, song, data, storage);
}
export async function reconcilePending(store, token, fetchImpl = fetch, storage = null) {
  for (const stale of await store.staleSubmissions()) {
    await store.settleSong(stale.id, 'failed', null, 'This request did not finish submitting. Your 100 credits have been returned.');
  }
  for (const job of await store.activeSongs()) {
    if (Date.now() - new Date(job.created_at).getTime() > 65 * 60 * 1000) {
      await store.settleSong(job.id, 'failed', null, 'This song expired before it could be retrieved. Your 100 credits have been returned.');
      continue;
    }
    try { await syncPrediction(store, job, token, fetchImpl, storage, true); } catch (_) { /* Retry at next interval. */ }
  }
  if (storage) await store.archiveLegacySongs(storage);
}
export function createServer({ token = process.env.REPLICATE_API_TOKEN,
  store = process.env.DATABASE_URL ? new Store(createPool()) : null,
  verifyGoogle = verifyGoogleIdToken, fetchImpl = fetch,
  supportEmail = process.env.CREDIT_SUPPORT_EMAIL || '',
  storage = createAudioStorage(),
  requireStorage = true,
} = {}) {
  return http.createServer(async (req, res) => {
    const origin = process.env.ALLOWED_ORIGIN || 'http://localhost:8080';
    if (req.headers.origin === origin) res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
    res.setHeader('Cache-Control', 'no-store');
    const send = (code, body) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(body)); };
    try {
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
    if (req.url === '/health' && req.method === 'GET') {
      send(200, { ok: true, configured: Boolean(store && token && token !== 'replace_with_your_token' && process.env.GOOGLE_WEB_CLIENT_ID), storage_configured: Boolean(storage) }); return;
    }
    if (!store) { send(503, { error: 'The studio database is not connected. Set DATABASE_URL and run migrations.' }); return; }
    if (req.method === 'POST' && req.url === '/api/auth/google') {
      try {
        const body = await readJson(req, 6000);
        if (typeof body?.id_token !== 'string' || body.id_token.length > 5000) { send(400, { error: 'A Google ID token is required.' }); return; }
        const identity = await verifyGoogle(body.id_token);
        const session = await store.signInGoogle(identity);
        send(200, { token: session.token, user: session.user, song_cost: SONG_COST, support_email: supportEmail });
      } catch (error) {
        send(error instanceof SyntaxError ? 400 : 401, { error: error instanceof SyntaxError ? 'Invalid JSON.' : 'Google sign-in could not be verified.' });
      }
      return;
    }
    const authToken = /^Bearer ([A-Za-z0-9_-]+)$/.exec(req.headers.authorization || '')?.[1];
    const user = await store.userForToken(authToken);
    if (!user) { send(401, { error: 'Please sign in with Google.' }); return; }
    if (req.method === 'POST' && req.url === '/api/auth/logout') { await store.signOut(authToken); send(200, { ok: true }); return; }
    if (req.method === 'GET' && req.url === '/api/me') { send(200, { user, song_cost: SONG_COST, support_email: supportEmail }); return; }
    if (req.method === 'GET' && req.url === '/api/credits') {
      send(200, { events: await store.creditHistory(user.id) }); return;
    }
    if (req.method === 'POST' && req.url === '/api/account/delete') {
      const body = await readJson(req);
      if (body.confirmation !== 'DELETE') { send(400, { error: 'Confirm account deletion.' }); return; }
      try { await store.deleteAccount(user.id, storage); }
      catch (error) { send(409, { error: error.message }); return; }
      send(200, { ok: true }); return;
    }
    if (req.method === 'GET' && req.url === '/api/songs') {
      send(200, { songs: await Promise.all((await store.listSongs(user.id)).map(row => presentSong(row, storage))) }); return;
    }
    if (req.method === 'POST' && req.url === '/api/songs') {
      if (!token || token === 'replace_with_your_token') { send(503, { error: 'The music model is not connected. Set REPLICATE_API_TOKEN.' }); return; }
      if (requireStorage && !storage) { send(503, { error: 'Song storage is not ready. Please try again later. No credits were charged.' }); return; }
      let input, title, style, settings, requestId;
      try {
        const body = await readJson(req);
        input = validateInput(body);
        requestId = body.request_id ?? null;
        if (requestId !== null && (typeof requestId !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(requestId))) throw new Error('Invalid request ID.');
        settings = { ...input, genre: String(body.genre ?? '').slice(0,80), mood: String(body.mood ?? '').slice(0,80),
          vocals: String(body.vocals ?? '').slice(0,80), bpm: Math.max(40,Math.min(220,Number(body.bpm)||100)), direction: String(body.direction ?? '').slice(0,2000) };
        title = typeof body.title === 'string' ? body.title.trim().slice(0, 80) : '';
        style = typeof body.style === 'string' ? body.style.trim().slice(0, 120) : '';
      } catch (e) { send(400, { error: e instanceof SyntaxError ? 'Invalid JSON.' : e.message }); return; }
      let reservation;
      try { reservation = await store.reserveSong(user.id, title || 'Untitled session', style || 'Custom sound', settings, requestId); }
      catch (e) {
        if (e instanceof InsufficientCredits) { send(402, { error: e.message, credits: (await store.getUser(user.id))?.credits ?? 0, song_cost: SONG_COST }); return; }
        send(500, { error: 'Could not reserve credits. Please try again.' }); return;
      }
      if (reservation.existing) {
        send(200, { ...await presentSong(await store.getSong(user.id, reservation.id), storage), credits: (await store.getUser(user.id)).credits }); return;
      }
      try {
        const upstream = await fetchImpl('https://api.replicate.com/v1/models/minimax/music-2.6/predictions', {
          method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ input }), signal: AbortSignal.timeout(35000),
        });
        if (!upstream.ok) throw new Error('Replicate rejected the song request. Your 100 credits have been returned.');
        const data = await upstream.json();
        if (!data.id) throw new Error('The music service returned no song ID. Your 100 credits have been returned.');
        await store.attachPrediction(reservation.id, data);
        if (['succeeded','failed','canceled'].includes(data.status)) {
          await finishPrediction(store, await store.getSong(user.id, reservation.id), data, storage);
        }
        const song = await store.getSong(user.id, reservation.id);
        send(200, { ...await presentSong(song, storage), credits: (await store.getUser(user.id)).credits });
      } catch (e) {
        await store.settleSong(reservation.id, 'failed', null, e.message);
        send(502, { error: e.message });
      }
      return;
    }
    const match = /^\/api\/songs\/([0-9a-f-]{36})$/.exec(req.url || '');
    if (req.method === 'GET' && match) {
      let song = await store.getSong(user.id, match[1]);
      if (!song) { send(404, { error: 'Song not found.' }); return; }
      try { song = await syncPrediction(store, song, token, fetchImpl, storage); } catch (_) { /* Background retry will settle it. */ }
      if (!song) { send(404, { error: 'Song not found.' }); return; }
      send(200, { ...await presentSong(song, storage), credits: (await store.getUser(user.id)).credits }); return;
    }
    const download = /^\/api\/songs\/([0-9a-f-]{36})\/download$/.exec(req.url || '');
    if (req.method === 'GET' && download) {
      const song = await store.getSong(user.id, download[1]);
      if (!song) { send(404, { error: 'Song not found.' }); return; }
      if (!song.storage_key || !storage) { send(409, { error: 'This audio is not saved yet. Please try again later.' }); return; }
      send(200, { url: await storage.url(song.storage_key, true) }); return;
    }
    send(404, { error: 'Not found.' });
    } catch (error) {
      console.error('Request failed:', error.message);
      if (!res.headersSent) send(500, { error: 'The studio had a temporary problem. Please try again.' });
      else res.end();
    }
  });
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const pool = createPool();
  await migrate(pool);
  const store = new Store(pool);
  const token = process.env.REPLICATE_API_TOKEN;
  const storage = createAudioStorage();
  const server = createServer({ store, token, storage });
  const port = Number(process.env.PORT || 8787), host = process.env.HOST || '127.0.0.1';
  server.listen(port, host, () => console.log(`Verse music backend listening at http://${host}:${port}`));
  let reconciling = false;
  const reconcile = async () => {
    if (reconciling) return;
    reconciling = true;
    try { await reconcilePending(store, token, fetch, storage); }
    catch (error) { console.error('Prediction reconciliation failed:', error.message); }
    finally { reconciling = false; }
  };
  void reconcile();
  const timer = setInterval(reconcile, 15000);
  timer.unref();
}
