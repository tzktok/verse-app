import { randomBytes, randomUUID, createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';

export const SONG_COST = 100;
export const WELCOME_CREDITS = 100;
const hash = value => createHash('sha256').update(value).digest('hex');
export const createPool = (connectionString = process.env.DATABASE_URL) => {
  if (!connectionString) throw new Error('DATABASE_URL is required.');
  return new pg.Pool({ connectionString, max: 10 });
};
export async function migrate(pool) {
  const sql = (await readFile(new URL('./schema.sql', import.meta.url), 'utf8')).replace(/^\uFEFF/, '');
  await pool.query(sql);
}
async function transaction(pool, work) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally { client.release(); }
}
export class InsufficientCredits extends Error {}
export class Store {
  constructor(pool) { this.pool = pool; }
  async signInGoogle({ sub, email, name }) {
    return transaction(this.pool, async client => {
      const created = await client.query(
        'INSERT INTO users(id,google_sub,email,display_name,credits) VALUES($1,$2,$3,$4,$5) ON CONFLICT (google_sub) DO NOTHING RETURNING id',
        [randomUUID(), sub, email, name, WELCOME_CREDITS],
      );
      if (created.rowCount) {
        await client.query('INSERT INTO credit_events(id,user_id,amount,kind,reference) VALUES($1,$2,$3,$4,$5)',
          [randomUUID(), created.rows[0].id, WELCOME_CREDITS, 'welcome', `welcome:${sub}`]);
      } else {
        await client.query('UPDATE users SET email=$2, display_name=$3, updated_at=now() WHERE google_sub=$1', [sub, email, name]);
      }
      const user = (await client.query('SELECT id,email,display_name,credits FROM users WHERE google_sub=$1', [sub])).rows[0];
      const token = randomBytes(32).toString('base64url');
      await client.query('INSERT INTO sessions(token_hash,user_id,expires_at) VALUES($1,$2,now()+interval \'30 days\')', [hash(token), user.id]);
      return { token, user };
    });
  }
  async userForToken(token) {
    if (!token || token.length > 256) return null;
    return (await this.pool.query('SELECT u.id,u.email,u.display_name,u.credits FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=$1 AND s.expires_at>now()', [hash(token)])).rows[0] ?? null;
  }
  async signOut(token) { await this.pool.query('DELETE FROM sessions WHERE token_hash=$1', [hash(token)]); }
  async getUser(id) { return (await this.pool.query('SELECT id,email,display_name,credits FROM users WHERE id=$1', [id])).rows[0] ?? null; }
  async listSongs(userId) {
    return (await this.pool.query('SELECT * FROM songs WHERE user_id=$1 ORDER BY created_at DESC', [userId])).rows;
  }
  async getSong(userId, songId) {
    return (await this.pool.query('SELECT * FROM songs WHERE user_id=$1 AND id=$2', [userId, songId])).rows[0] ?? null;
  }
  async reserveSong(userId, title, style, settings = {}, requestId = null) {
    return transaction(this.pool, async client => {
      await client.query('SELECT id FROM users WHERE id=$1 FOR UPDATE', [userId]);
      if (requestId) {
        const previous = (await client.query('SELECT id FROM songs WHERE user_id=$1 AND request_id=$2', [userId, requestId])).rows[0];
        if (previous) return { id: previous.id, existing: true };
      }
      const debited = await client.query('UPDATE users SET credits=credits-$2,updated_at=now() WHERE id=$1 AND credits >= $2 RETURNING credits', [userId, SONG_COST]);
      if (!debited.rowCount) throw new InsufficientCredits('You need 100 credits to create a song.');
      const id = randomUUID();
      await client.query('INSERT INTO songs(id,user_id,title,style,status,settings,request_id) VALUES($1,$2,$3,$4,$5,$6,$7)', [id, userId, title, style, 'submitting', JSON.stringify(settings), requestId]);
      await client.query('INSERT INTO credit_events(id,user_id,amount,kind,reference) VALUES($1,$2,$3,$4,$5)', [randomUUID(), userId, -SONG_COST, 'song', `song:${id}`]);
      return { id, credits: debited.rows[0].credits };
    });
  }
  async attachPrediction(id, prediction) {
    await this.pool.query('UPDATE songs SET replicate_id=$2,status=$3,updated_at=now() WHERE id=$1 AND status=$4', [id, prediction.id, 'starting', 'submitting']);
  }
  async settleSong(id, status, audioUrl = null, error = null) {
    if (status === 'succeeded' && !audioUrl) {
      status = 'failed';
      error = 'The model returned no playable audio. Your 100 credits have been returned.';
    }
    return transaction(this.pool, async client => {
      const row = (await client.query('SELECT * FROM songs WHERE id=$1 FOR UPDATE', [id])).rows[0];
      if (!row) return null;
      if (['succeeded', 'failed', 'canceled'].includes(row.status)) return row;
      if (row.status === 'saving' && ['starting','processing'].includes(status)) return row;
      if (!['starting','processing','succeeded','failed','canceled'].includes(status)) return row;
      await client.query('UPDATE songs SET status=$2,audio_url=$3,error=$4,updated_at=now() WHERE id=$1', [id,status,audioUrl,error]);
      if (['failed','canceled'].includes(status) && row.charged) {
        await client.query('UPDATE users SET credits=credits+$2,updated_at=now() WHERE id=$1', [row.user_id, SONG_COST]);
        await client.query('UPDATE songs SET charged=false WHERE id=$1', [id]);
        await client.query('INSERT INTO credit_events(id,user_id,amount,kind,reference) VALUES($1,$2,$3,$4,$5)', [randomUUID(),row.user_id,SONG_COST,'refund',`refund:${id}`]);
      }
      return (await client.query('SELECT * FROM songs WHERE id=$1', [id])).rows[0];
    });
  }
  async staleSubmissions() {
    return (await this.pool.query("SELECT id FROM songs WHERE status='submitting' AND created_at < now()-interval '2 minutes' LIMIT 50")).rows;
  }
  async activeSongs() {
    return (await this.pool.query("SELECT * FROM songs WHERE status IN ('starting','processing','saving') AND replicate_id IS NOT NULL ORDER BY updated_at ASC LIMIT 50")).rows;
  }
  async markSaving(id, source) {
    return (await this.pool.query("UPDATE songs SET status='saving',source_url=$2,updated_at=now() WHERE id=$1 AND status IN ('starting','processing') RETURNING *", [id, source])).rows[0];
  }
  async saveAudio(id, storage) {
    return transaction(this.pool, async client => {
      const song = (await client.query('SELECT * FROM songs WHERE id=$1 FOR UPDATE', [id])).rows[0];
      if (!song || song.status !== 'saving') return song;
      const key = await storage.save(song, song.source_url);
      return (await client.query("UPDATE songs SET storage_key=$2,status='succeeded',audio_url=null,source_url=null,error=null,updated_at=now() WHERE id=$1 RETURNING *", [id, key])).rows[0];
    });
  }
  async creditHistory(userId) {
    return (await this.pool.query(`SELECT e.id,e.amount,e.kind,e.created_at,s.title AS song_title
      FROM credit_events e LEFT JOIN songs s ON s.user_id=e.user_id AND (e.reference='song:'||s.id::text OR e.reference='refund:'||s.id::text)
      WHERE e.user_id=$1 ORDER BY e.created_at DESC LIMIT 200`, [userId])).rows;
  }
  async archiveLegacySongs(storage) {
    const rows = (await this.pool.query("SELECT id FROM songs WHERE status='succeeded' AND storage_key IS NULL AND audio_url IS NOT NULL AND error IS NULL LIMIT 10")).rows;
    for (const { id } of rows) {
      await transaction(this.pool, async client => {
        const song = (await client.query('SELECT * FROM songs WHERE id=$1 FOR UPDATE',[id])).rows[0];
        if (!song || song.storage_key || song.error) return;
        try {
          const key = await storage.save(song, song.audio_url);
          await client.query('UPDATE songs SET storage_key=$2,audio_url=null WHERE id=$1',[id,key]);
        } catch (_) {
          await client.query('UPDATE songs SET error=$2 WHERE id=$1',[id,'This older audio could not be imported. Contact support if you have a downloaded copy.']);
        }
      });
    }
  }
  async deleteAccount(userId, storage) {
    return transaction(this.pool, async client => {
      await client.query('SELECT id FROM users WHERE id=$1 FOR UPDATE', [userId]);
      const songs = (await client.query('SELECT * FROM songs WHERE user_id=$1 FOR UPDATE', [userId])).rows;
      if (songs.some(song => ['submitting','starting','processing','saving'].includes(song.status))) {
        throw new Error('Wait for your current songs to finish before deleting your account.');
      }
      if (songs.some(song => song.storage_key) && !storage) throw new Error('Audio storage is unavailable. Please try again later.');
      if (storage) await storage.deleteUser(userId);
      await client.query('DELETE FROM users WHERE id=$1', [userId]);
    });
  }
}
