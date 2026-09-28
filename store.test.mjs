import test from 'node:test';
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { Store, migrate, InsufficientCredits } from './store.mjs';

async function setup() {
  const db = new PGlite();
  const pool = {
    query: (sql, values) => sql.startsWith('CREATE TABLE') ? db.exec(sql) : db.query(sql, values),
    connect: async () => ({ query: (sql, values) => db.query(sql, values), release() {} }),
  };
  await migrate(pool);
  return { pool, store: new Store(pool), close: () => db.close() };
}
test('welcome credits are granted once per Google subject', async () => {
  const { pool, store, close } = await setup();
  const first = await store.signInGoogle({ sub: 'google-1', email: 'one@example.com', name: 'One' });
  const again = await store.signInGoogle({ sub: 'google-1', email: 'new@example.com', name: 'Updated' });
  assert.equal(first.user.id, again.user.id);
  assert.equal(again.user.credits, 100);
  assert.equal((await pool.query("SELECT count(*)::int AS count FROM credit_events WHERE kind='welcome'")).rows[0].count, 1);
  assert.equal((await store.userForToken(first.token)).email, 'new@example.com');
  await store.signOut(first.token);
  assert.equal(await store.userForToken(first.token), null);
  await close();
});
test('a song costs 100 credits; failed songs refund only once', async () => {
  const { pool, store, close } = await setup();
  const { user } = await store.signInGoogle({ sub: 'google-2', email: 'two@example.com', name: 'Two' });
  const reserved = await store.reserveSong(user.id, 'Title', 'Style');
  assert.equal(reserved.credits, 0);
  await assert.rejects(store.reserveSong(user.id, 'Second', 'Style'), InsufficientCredits);
  await store.attachPrediction(reserved.id, { id: 'replicate-123', status: 'starting' });
  await store.settleSong(reserved.id, 'failed', null, 'Model failed');
  await store.settleSong(reserved.id, 'failed', null, 'Model failed');
  assert.equal((await store.getUser(user.id)).credits, 100);
  assert.equal((await pool.query('SELECT count(*)::int AS count FROM credit_events WHERE user_id=$1', [user.id])).rows[0].count, 3);
  await close();
});
test('direct database grant updates credits once and records the ledger', async () => {
  const { pool, store, close } = await setup();
  const { user } = await store.signInGoogle({ sub: 'google-3', email: 'three@example.com', name: 'Three' });
  const grant = (credits, reference, userId = user.id) =>
    pool.query('SELECT grant_manual_credits($1,$2,$3) AS balance', [userId, credits, reference]);
  assert.equal((await grant(500, 'case_1234')).rows[0].balance, 600);
  await assert.rejects(grant(500, 'case_1234'), /reference already used/);
  assert.equal((await store.getUser(user.id)).credits, 600);
  assert.equal((await pool.query("SELECT amount FROM credit_events WHERE reference='manual:case_1234'")).rows[0].amount, 500);
  await assert.rejects(grant(500, 'new_case', '00000000-0000-0000-0000-000000000000'), /Account not found/);
  await assert.rejects(grant(-1, 'invalid_case'), /Credits must be/);
  await close();
});
test('songs are scoped to their owner', async () => {
  const { store, close } = await setup();
  const a = await store.signInGoogle({ sub: 'a', email: 'a@example.com', name: 'A' });
  const b = await store.signInGoogle({ sub: 'b', email: 'b@example.com', name: 'B' });
  const song = await store.reserveSong(a.user.id, 'Private song', 'Dreamy');
  assert.equal((await store.listSongs(a.user.id)).length, 1);
  assert.equal((await store.listSongs(b.user.id)).length, 0);
  assert.equal(await store.getSong(b.user.id, song.id), null);
  await close();
});

test('failed audio deletion retains the account for retry, and saving cannot regress',async()=>{
  const {store,close}=await setup();
  try {
    const {user}=await store.signInGoogle({sub:'deletion-test',email:'delete@example.com',name:'Delete'});
    const song=await store.reserveSong(user.id,'Song','Piano');
    await store.attachPrediction(song.id,{id:'upstream'});
    await store.markSaving(song.id,'https://replicate.delivery/audio.mp3');
    await store.settleSong(song.id,'processing');
    assert.equal((await store.getSong(user.id,song.id)).status,'saving');
    await store.saveAudio(song.id,{save:async()=>`users/${user.id}/songs/${song.id}.mp3`});
    await assert.rejects(store.deleteAccount(user.id,{deleteUser:async()=>{throw new Error('R2 offline');}}),/R2 offline/);
    assert.ok(await store.getUser(user.id));
    assert.equal((await store.listSongs(user.id)).length,1);
  } finally {await close();}
});
