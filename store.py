import hashlib
import json
import os
import secrets
import uuid
from pathlib import Path

import asyncpg

SONG_COST = 100
WELCOME_CREDITS = 100

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog", format="text"
    )


async def create_pool(dsn: str | None = None) -> asyncpg.Pool:
    dsn = dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is required.")
    return await asyncpg.create_pool(dsn, min_size=1, max_size=10, init=_init_connection)


async def migrate(pool: asyncpg.Pool) -> None:
    sql = _SCHEMA_PATH.read_text(encoding="utf-8").lstrip("﻿")
    await pool.execute(sql)


class InsufficientCredits(Exception):
    pass


def _user_public(record) -> dict | None:
    if record is None:
        return None
    return {
        "id": str(record["id"]),
        "email": record["email"],
        "display_name": record["display_name"],
        "credits": record["credits"],
    }


def _song_dict(record) -> dict | None:
    if record is None:
        return None
    data = dict(record)
    # jsonb round-trips as a Python dict already thanks to the codec above,
    # but guard against a plain string in case a codec-less connection is used.
    settings = data.get("settings")
    if isinstance(settings, str):
        data["settings"] = json.loads(settings) if settings else {}
    elif settings is None:
        data["settings"] = {}
    return data


class Store:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def sign_in_google(self, sub: str, email: str, name: str) -> dict:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                new_id = uuid.uuid4()
                created = await conn.fetchrow(
                    "INSERT INTO users(id,google_sub,email,display_name,credits) VALUES($1,$2,$3,$4,$5) "
                    "ON CONFLICT (google_sub) DO NOTHING RETURNING id",
                    new_id, sub, email, name, WELCOME_CREDITS,
                )
                if created:
                    await conn.execute(
                        "INSERT INTO credit_events(id,user_id,amount,kind,reference) VALUES($1,$2,$3,$4,$5)",
                        uuid.uuid4(), created["id"], WELCOME_CREDITS, "welcome", f"welcome:{sub}",
                    )
                else:
                    await conn.execute(
                        "UPDATE users SET email=$2, display_name=$3, updated_at=now() WHERE google_sub=$1",
                        sub, email, name,
                    )
                user = await conn.fetchrow(
                    "SELECT id,email,display_name,credits FROM users WHERE google_sub=$1", sub
                )
                token = secrets.token_urlsafe(32)
                await conn.execute(
                    "INSERT INTO sessions(token_hash,user_id,expires_at) VALUES($1,$2,now()+interval '30 days')",
                    _hash(token), user["id"],
                )
                return {"token": token, "user": _user_public(user)}

    async def user_for_token(self, token: str | None) -> dict | None:
        if not token or len(token) > 256:
            return None
        row = await self.pool.fetchrow(
            "SELECT u.id,u.email,u.display_name,u.credits FROM sessions s JOIN users u ON u.id=s.user_id "
            "WHERE s.token_hash=$1 AND s.expires_at>now()",
            _hash(token),
        )
        return _user_public(row)

    async def sign_out(self, token: str) -> None:
        await self.pool.execute("DELETE FROM sessions WHERE token_hash=$1", _hash(token))

    async def get_user(self, user_id) -> dict | None:
        row = await self.pool.fetchrow(
            "SELECT id,email,display_name,credits FROM users WHERE id=$1", user_id
        )
        return _user_public(row)

    async def list_songs(self, user_id) -> list[dict]:
        rows = await self.pool.fetch(
            "SELECT * FROM songs WHERE user_id=$1 ORDER BY created_at DESC", user_id
        )
        return [_song_dict(r) for r in rows]

    async def get_song(self, user_id, song_id) -> dict | None:
        row = await self.pool.fetchrow(
            "SELECT * FROM songs WHERE user_id=$1 AND id=$2", user_id, song_id
        )
        return _song_dict(row)

    async def reserve_song(self, user_id, title, style, settings=None, request_id=None) -> dict:
        settings = settings or {}
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow("SELECT id FROM users WHERE id=$1 FOR UPDATE", user_id)
                if request_id:
                    previous = await conn.fetchrow(
                        "SELECT id FROM songs WHERE user_id=$1 AND request_id=$2", user_id, request_id
                    )
                    if previous:
                        return {"id": previous["id"], "existing": True, "credits": None}
                debited = await conn.fetchrow(
                    "UPDATE users SET credits=credits-$2,updated_at=now() WHERE id=$1 AND credits >= $2 "
                    "RETURNING credits",
                    user_id, SONG_COST,
                )
                if not debited:
                    raise InsufficientCredits("You need 100 credits to create a song.")
                song_id = uuid.uuid4()
                await conn.execute(
                    "INSERT INTO songs(id,user_id,title,style,status,settings,request_id) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7)",
                    song_id, user_id, title, style, "submitting", settings, request_id,
                )
                await conn.execute(
                    "INSERT INTO credit_events(id,user_id,amount,kind,reference) VALUES($1,$2,$3,$4,$5)",
                    uuid.uuid4(), user_id, -SONG_COST, "song", f"song:{song_id}",
                )
                return {"id": song_id, "existing": False, "credits": debited["credits"]}

    async def attach_prediction(self, song_id, prediction: dict) -> None:
        await self.pool.execute(
            "UPDATE songs SET replicate_id=$2,status=$3,updated_at=now() WHERE id=$1 AND status=$4",
            song_id, prediction.get("id"), "starting", "submitting",
        )

    async def settle_song(self, song_id, status, audio_url=None, error=None) -> dict | None:
        if status == "succeeded" and not audio_url:
            status = "failed"
            error = "The model returned no playable audio. Your 100 credits have been returned."
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("SELECT * FROM songs WHERE id=$1 FOR UPDATE", song_id)
                if not row:
                    return None
                if row["status"] in ("succeeded", "failed", "canceled"):
                    return _song_dict(row)
                if row["status"] == "saving" and status in ("starting", "processing"):
                    return _song_dict(row)
                if status not in ("starting", "processing", "succeeded", "failed", "canceled"):
                    return _song_dict(row)
                await conn.execute(
                    "UPDATE songs SET status=$2,audio_url=$3,error=$4,updated_at=now() WHERE id=$1",
                    song_id, status, audio_url, error,
                )
                if status in ("failed", "canceled") and row["charged"]:
                    await conn.execute(
                        "UPDATE users SET credits=credits+$2,updated_at=now() WHERE id=$1",
                        row["user_id"], SONG_COST,
                    )
                    await conn.execute("UPDATE songs SET charged=false WHERE id=$1", song_id)
                    await conn.execute(
                        "INSERT INTO credit_events(id,user_id,amount,kind,reference) VALUES($1,$2,$3,$4,$5)",
                        uuid.uuid4(), row["user_id"], SONG_COST, "refund", f"refund:{song_id}",
                    )
                updated = await conn.fetchrow("SELECT * FROM songs WHERE id=$1", song_id)
                return _song_dict(updated)

    async def stale_submissions(self) -> list[dict]:
        rows = await self.pool.fetch(
            "SELECT id FROM songs WHERE status='submitting' AND created_at < now()-interval '2 minutes' LIMIT 50"
        )
        return [dict(r) for r in rows]

    async def active_songs(self) -> list[dict]:
        rows = await self.pool.fetch(
            "SELECT * FROM songs WHERE status IN ('starting','processing','saving') "
            "AND replicate_id IS NOT NULL ORDER BY updated_at ASC LIMIT 50"
        )
        return [_song_dict(r) for r in rows]

    async def mark_saving(self, song_id, source) -> dict | None:
        row = await self.pool.fetchrow(
            "UPDATE songs SET status='saving',source_url=$2,updated_at=now() "
            "WHERE id=$1 AND status IN ('starting','processing') RETURNING *",
            song_id, source,
        )
        return _song_dict(row)

    async def save_audio(self, song_id, storage) -> dict | None:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("SELECT * FROM songs WHERE id=$1 FOR UPDATE", song_id)
                if not row or row["status"] != "saving":
                    return _song_dict(row)
                key = await storage.save(dict(row), row["source_url"])
                updated = await conn.fetchrow(
                    "UPDATE songs SET storage_key=$2,status='succeeded',audio_url=null,source_url=null,"
                    "error=null,updated_at=now() WHERE id=$1 RETURNING *",
                    song_id, key,
                )
                return _song_dict(updated)

    async def credit_history(self, user_id) -> list[dict]:
        rows = await self.pool.fetch(
            """SELECT e.id,e.amount,e.kind,e.created_at,s.title AS song_title
               FROM credit_events e LEFT JOIN songs s ON s.user_id=e.user_id
                 AND (e.reference='song:'||s.id::text OR e.reference='refund:'||s.id::text)
               WHERE e.user_id=$1 ORDER BY e.created_at DESC LIMIT 200""",
            user_id,
        )
        return [dict(r) for r in rows]

    async def archive_legacy_songs(self, storage) -> None:
        rows = await self.pool.fetch(
            "SELECT id FROM songs WHERE status='succeeded' AND storage_key IS NULL "
            "AND audio_url IS NOT NULL AND error IS NULL LIMIT 10"
        )
        for row in rows:
            song_id = row["id"]
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    song = await conn.fetchrow("SELECT * FROM songs WHERE id=$1 FOR UPDATE", song_id)
                    if not song or song["storage_key"] or song["error"]:
                        continue
                    try:
                        key = await storage.save(dict(song), song["audio_url"])
                        await conn.execute(
                            "UPDATE songs SET storage_key=$2,audio_url=null WHERE id=$1", song_id, key
                        )
                    except Exception:
                        await conn.execute(
                            "UPDATE songs SET error=$2 WHERE id=$1",
                            song_id,
                            "This older audio could not be imported. Contact support if you have a downloaded copy.",
                        )

    async def delete_account(self, user_id, storage) -> None:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow("SELECT id FROM users WHERE id=$1 FOR UPDATE", user_id)
                songs = await conn.fetch("SELECT * FROM songs WHERE user_id=$1 FOR UPDATE", user_id)
                if any(s["status"] in ("submitting", "starting", "processing", "saving") for s in songs):
                    raise RuntimeError("Wait for your current songs to finish before deleting your account.")
                if any(s["storage_key"] for s in songs) and not storage:
                    raise RuntimeError("Audio storage is unavailable. Please try again later.")
                if storage:
                    await storage.delete_user(user_id)
                await conn.execute("DELETE FROM users WHERE id=$1", user_id)
