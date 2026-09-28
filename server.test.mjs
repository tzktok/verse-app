import test from 'node:test';
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { Store, migrate } from './store.mjs';
import { createServer, validateInput, audioUrl, reconcilePending } from './server.mjs';

async function fixture(fetchImpl, replicateToken = 'private-test-token', options = {}) {
  const db = new PGlite();
  const pool = { query: (sql, values) => sql.startsWith('CREATE TABLE') ? db.exec(sql) : db.query(sql, values),
    connect: async () => ({ query: (sql, values) => db.query(sql, values), release() {} }) };
  await migrate(pool);
  const store = new Store(pool);
  const server = createServer({ store, token: replicateToken, fetchImpl, storage: null, requireStorage: false, ...options,
    verifyGoogle: async token => {
      if (token === 'bad') throw new Error('Invalid token');
      return {sub: token, email: `${token}@example.com`, name: token};
    }, supportEmail: 'support@example.com' });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  const close = async () => { await new Promise(resolve => server.close(resolve)); await db.close(); };
  const login = async token => {
    const response = await fetch(`${base}/api/auth/google`, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id_token:token})});
    assert.equal(response.status,200);
    return response.json();
  };
  return {base,close,login,store};
}
const postSong = (base, token, body = { lyrics:'[Verse]\nHello world', prompt:'Dreamy indie pop', title:'First song', style:'Indie pop · Dreamy' }) =>
  fetch(`${base}/api/songs`, {method:'POST',headers:{'Content-Type':'application/json',Authorization:`Bearer ${token}`},body:JSON.stringify(body)});

test('model input is validated before billing', () => {
  assert.equal(validateInput({lyrics:'[Verse]\nHello'}).audio_format,'mp3');
  assert.throws(() => validateInput({lyrics:''}),/Add lyrics/);
  assert.throws(() => validateInput({lyrics:'x'.repeat(3501)}),/3,500/);
  assert.throws(() => validateInput({is_instrumental:true}),/Describe/);
  assert.equal(validateInput({is_instrumental:true,prompt:'Piano'}).is_instrumental,true);
  assert.equal(validateInput({lyrics_optimizer:true,prompt:'Dreamy'}).lyrics_optimizer,true);
  assert.equal(audioUrl(['https://example.com/song.mp3']),'https://example.com/song.mp3');
  assert.equal(audioUrl('file:///secret'),null);
});

test('R2 saves by owner, retries safely, refreshes links, and scopes downloads/history/deletion', async () => {
  let fail = true, writes = 0, links = 0;
  const deleted=[];
  const storage={
    save:async song=>{ writes++; if(fail) throw new Error('R2 temporarily offline'); return `users/${song.user_id}/songs/${song.id}.mp3`; },
    url:async key=>`https://private.example/${key}?signature=${++links}`,
    deleteUser:async id=>{deleted.push(id);},
  };
  const upstream=async()=>({ok:true,json:async()=>({id:'persistent-song',status:'succeeded',output:'https://replicate.delivery/test.mp3'})});
  const f=await fixture(upstream,'test',{storage,requireStorage:true});
  try {
    const a=await f.login('owner'), b=await f.login('other');
    const headers={Authorization:`Bearer ${a.token}`};
    const body={lyrics:'Original lyrics',prompt:'Piano',request_id:'11111111-1111-4111-8111-111111111111'};
    const song=await(await postSong(f.base,a.token,body)).json();
    assert.equal(song.status,'saving');
    assert.equal((await(await postSong(f.base,a.token,body)).json()).id,song.id);
    assert.equal((await f.store.getUser(a.user.id)).credits,0);
    await reconcilePending(f.store,'test',upstream,storage);
    assert.equal((await f.store.getSong(a.user.id,song.id)).status,'saving');
    fail=false;
    await reconcilePending(f.store,'test',upstream,storage);
    const saved=await f.store.getSong(a.user.id,song.id);
    assert.equal(saved.storage_key,`users/${a.user.id}/songs/${song.id}.mp3`);
    assert.equal(saved.source_url,null);
    assert.equal(saved.settings.lyrics,'Original lyrics');
    assert.equal(writes,2);
    await reconcilePending(f.store,'test',upstream,storage);
    assert.equal(writes,2);
    const url=`${f.base}/api/songs/${song.id}/download`;
    assert.equal((await fetch(url,{headers:{Authorization:`Bearer ${b.token}`}})).status,404);
    const first=await(await fetch(url,{headers})).json(), second=await(await fetch(url,{headers})).json();
    assert.notEqual(first.url,second.url);
    const events=await(await fetch(`${f.base}/api/credits`,{headers})).json();
    assert.equal(events.events.length,2);
    assert.equal((await(await fetch(`${f.base}/api/credits`,{headers:{Authorization:`Bearer ${b.token}`}})).json()).events.length,1);
    const deletion=await fetch(`${f.base}/api/account/delete`,{method:'POST',headers:{...headers,'Content-Type':'application/json'},body:JSON.stringify({confirmation:'DELETE'})});
    assert.equal(deletion.status,200);
    assert.deepEqual(deleted,[a.user.id]);
    assert.equal(await f.store.userForToken(a.token),null);
    assert.equal((await f.store.listSongs(a.user.id)).length,0);
    assert.equal((await f.store.creditHistory(a.user.id)).length,0);
    assert.ok(await f.store.getUser(b.user.id));
  } finally { await f.close(); }
});

test('missing R2 prevents charges, and unfinished songs prevent account deletion',async()=>{
  const f=await fixture(async()=>{throw new Error('Unexpected model call');},'test',{storage:null,requireStorage:true});
  try {
    const auth=await f.login('safe');
    assert.equal((await postSong(f.base,auth.token)).status,503);
    assert.equal((await f.store.getUser(auth.user.id)).credits,100);
    await f.store.reserveSong(auth.user.id,'Working','Piano');
    await assert.rejects(f.store.deleteAccount(auth.user.id,null),/Wait for/);
    assert.ok(await f.store.getUser(auth.user.id));
  } finally { await f.close(); }
});
test('authenticated song request costs 100, blocks a second request, and polls owner-only output', async () => {
  const calls=[];
  const f=await fixture(async (url,options) => { calls.push({url,options}); return {ok:true,json:async()=> options.method==='POST'
    ? {id:'replicate-123',status:'starting'} : {id:'replicate-123',status:'succeeded',output:'https://example.com/song.mp3'} }; });
  try {
    assert.equal((await fetch(`${f.base}/api/me`)).status,401);
    const auth=await f.login('alice');
    assert.equal(auth.user.credits,100);
    assert.equal(auth.support_email,'support@example.com');
    const invalid=await postSong(f.base,auth.token,{lyrics:'',prompt:''});
    assert.equal(invalid.status,400);
    assert.equal((await f.store.getUser(auth.user.id)).credits,100);
    const created=await postSong(f.base,auth.token);
    assert.equal(created.status,200);
    const song=await created.json();
    assert.equal(song.credits,0);
    assert.match(song.id,/^[0-9a-f-]{36}$/);
    assert.equal(calls[0].options.headers.Authorization,'Bearer private-test-token');
    assert.equal(JSON.parse(calls[0].options.body).input.lyrics,'[Verse]\nHello world');
    const blocked=await postSong(f.base,auth.token);
    assert.equal(blocked.status,402);
    assert.equal(calls.length,1);
    const other=await f.login('bob');
    assert.equal((await fetch(`${f.base}/api/songs/${song.id}`,{headers:{Authorization:`Bearer ${other.token}`}})).status,404);
    const result=await (await fetch(`${f.base}/api/songs/${song.id}`,{headers:{Authorization:`Bearer ${auth.token}`}})).json();
    assert.equal(result.status,'succeeded');
    assert.equal(result.audio_url,'https://example.com/song.mp3');
    assert.equal((await f.store.getUser(auth.user.id)).credits,0);
  } finally { await f.close(); }
});
test('model failure returns reserved credits once', async () => {
  const f=await fixture(async (url,options) => ({ok:true,json:async()=> options.method==='POST'
    ? {id:'replicate-failed',status:'starting'} : {id:'replicate-failed',status:'failed',error:'Model failed'} }));
  try {
    const auth=await f.login('carol');
    const song=await (await postSong(f.base,auth.token)).json();
    const path=`${f.base}/api/songs/${song.id}`;
    const headers={Authorization:`Bearer ${auth.token}`};
    assert.equal((await (await fetch(path,{headers})).json()).status,'failed');
    assert.equal((await (await fetch(path,{headers})).json()).status,'failed');
    assert.equal((await f.store.getUser(auth.user.id)).credits,100);
  } finally { await f.close(); }
});
test('invalid Google token is rejected and cannot create an account', async () => {
  const f=await fixture(async()=>{throw new Error('Should not call Replicate');});
  try {
    const result=await fetch(`${f.base}/api/auth/google`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id_token:'bad'})});
    assert.equal(result.status,401);
    assert.equal((await f.store.pool.query('SELECT count(*)::int AS count FROM users')).rows[0].count,0);
  } finally { await f.close(); }
});
test('missing Replicate configuration never charges credits', async () => {
  const f = await fixture(async () => { throw new Error('Replicate should not be called'); }, '');
  try {
    const auth = await f.login('dana');
    assert.equal((await postSong(f.base, auth.token)).status, 503);
    assert.equal((await f.store.getUser(auth.user.id)).credits, 100);
  } finally { await f.close(); }
});
test('a completed prediction without audio refunds credits', async () => {
  const f = await fixture(async () => ({ ok: true, json: async () => ({ id: 'replicate-empty', status: 'succeeded', output: null }) }));
  try {
    const auth = await f.login('erin');
    const response = await postSong(f.base, auth.token);
    assert.equal(response.status, 200);
    const song = await response.json();
    assert.equal(song.status, 'failed');
    assert.equal(song.credits, 100);
  } finally { await f.close(); }
});
