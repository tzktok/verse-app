import test from 'node:test';
import assert from 'node:assert/strict';
import { createAudioStorage } from './audio-storage.mjs';

const env = { R2_ACCOUNT_ID:'testaccount',R2_BUCKET_NAME:'private-songs',R2_ACCESS_KEY_ID:'testkey',R2_SECRET_ACCESS_KEY:'testsecret' };
test('private R2 links are signed with a bounded expiry and download disposition', async()=>{
  const storage=createAudioStorage(env);
  const url=new URL(await storage.url('users/user-id/songs/song-id.mp3',true));
  assert.equal(url.protocol,'https:');
  assert.equal(url.hostname,'private-songs.testaccount.r2.cloudflarestorage.com');
  assert.equal(url.searchParams.get('X-Amz-Expires'),'3600');
  assert.match(url.searchParams.get('response-content-disposition'),/^attachment/);
  assert.ok(url.searchParams.has('X-Amz-Signature'));
});
test('R2 requires all credentials and rejects arbitrary audio sources before fetching',async()=>{
  assert.equal(createAudioStorage({}),null);
  const storage=createAudioStorage(env);
  for (const source of ['http://replicate.delivery/a.mp3','https://127.0.0.1/a.mp3','https://replicate.delivery.attacker.test/a.mp3']) {
    await assert.rejects(storage.save({id:'song',user_id:'user'},source),/Unsupported audio source/);
  }
});
