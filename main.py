import asyncio
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token
from starlette.exceptions import HTTPException as StarletteHTTPException

from audio_storage import create_audio_storage
from store import InsufficientCredits, SONG_COST, Store, create_pool, migrate

load_dotenv()

_BEARER_RE = re.compile(r"^Bearer ([A-Za-z0-9_-]+)$")
_REQUEST_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_SONG_ID_RE = re.compile(r"^[0-9a-f-]{36}$", re.IGNORECASE)
_google_request = google_requests.Request()


def err(status: int, message: str, **extra) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": message, **extra})


async def read_json_limited(request: Request, max_bytes: int = 24000):
    content_type = request.headers.get("content-type", "")
    if not content_type.startswith("application/json"):
        raise ValueError("Content-Type must be application/json.")
    body = b""
    async for chunk in request.stream():
        body += chunk
        if len(body) > max_bytes:
            raise ValueError("Request is too large.")
    return json.loads(body)


async def verify_google_id_token(token: str, audience: str | None = None) -> dict:
    audience = audience or os.environ.get("GOOGLE_WEB_CLIENT_ID")
    if not audience:
        raise ValueError("GOOGLE_WEB_CLIENT_ID is required.")
    payload = await asyncio.to_thread(google_id_token.verify_oauth2_token, token, _google_request, audience)
    if not payload.get("sub") or not payload.get("email") or not payload.get("email_verified"):
        raise ValueError("Google account has no verified email.")
    name = payload.get("name") or payload["email"].split("@")[0]
    return {"sub": payload["sub"], "email": payload["email"], "name": name}


def validate_input(body) -> dict:
    if not isinstance(body, dict):
        raise ValueError("Invalid song request.")
    lyrics = body.get("lyrics", "")
    prompt = body.get("prompt", "")
    is_instrumental = body.get("is_instrumental", False)
    lyrics_optimizer = body.get("lyrics_optimizer", False)
    if not isinstance(lyrics, str) or len(lyrics) > 3500:
        raise ValueError("Lyrics must be text with at most 3,500 characters.")
    if not isinstance(prompt, str) or len(prompt) > 2000:
        raise ValueError("Sound direction must be text with at most 2,000 characters.")
    if not isinstance(is_instrumental, bool) or not isinstance(lyrics_optimizer, bool):
        raise ValueError("Invalid music options.")
    if not is_instrumental and not lyrics_optimizer and not lyrics.strip():
        raise ValueError("Add lyrics to create a vocal track.")
    if (is_instrumental or lyrics_optimizer) and not prompt.strip():
        raise ValueError("Describe the sound you want to create.")
    return {
        "lyrics": lyrics,
        "prompt": prompt,
        "is_instrumental": is_instrumental,
        "lyrics_optimizer": lyrics_optimizer,
        "audio_format": "mp3",
        "sample_rate": 44100,
        "bitrate": 256000,
    }


def audio_url(output):
    if isinstance(output, str) and output.startswith("https://"):
        return output
    if isinstance(output, list):
        for item in output:
            found = audio_url(item)
            if found:
                return found
        return None
    if isinstance(output, dict):
        return audio_url(output.get("audio") or output.get("url") or output.get("audio_file"))
    return None


def _is_request_id(value) -> bool:
    return isinstance(value, str) and bool(_REQUEST_ID_RE.match(value))


def _js_number_or(value, default):
    # Replicates `Number(x) || default`: NaN *and* 0 both fall back, matching
    # the original Node behavior exactly (a bpm of 0 was never a real input).
    try:
        n = float(value)
    except (TypeError, ValueError):
        return default
    if n != n:  # NaN
        return default
    return n if n else default


def public_song(row: dict) -> dict:
    return {
        "id": row["id"],
        "title": row["title"],
        "style": row["style"],
        "status": row["status"],
        "audio_url": row.get("audio_url"),
        "error": row.get("error"),
        "created_at": row.get("created_at"),
        "settings": row.get("settings") or {},
        "stored": bool(row.get("storage_key")),
    }


async def present_song(row: dict, storage) -> dict:
    result = public_song(row)
    if row.get("storage_key"):
        result["audio_url"] = await storage.url(row["storage_key"]) if storage else None
    return result


async def finish_prediction(store: Store, song: dict, data: dict, storage):
    source = audio_url(data.get("output"))
    if data.get("status") == "succeeded" and source and storage:
        await store.mark_saving(song["id"], source)
        return await store.get_song(song["user_id"], song["id"])
    return await store.settle_song(
        song["id"],
        data.get("status"),
        source,
        "Generation failed. Your 100 credits have been returned." if data.get("error") else None,
    )


async def sync_prediction(store: Store, song: dict, token, http_client: httpx.AsyncClient, storage, persist=False):
    if song["status"] == "saving":
        return await store.save_audio(song["id"], storage) if (storage and persist) else song
    if not song.get("replicate_id") or song["status"] not in ("starting", "processing"):
        return song
    upstream = await http_client.get(
        f"https://api.replicate.com/v1/predictions/{song['replicate_id']}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=35.0,
    )
    if upstream.status_code >= 400:
        return song
    return await finish_prediction(store, song, upstream.json(), storage)


async def reconcile_pending(store: Store, token, http_client: httpx.AsyncClient, storage=None):
    for stale in await store.stale_submissions():
        await store.settle_song(
            stale["id"], "failed", None,
            "This request did not finish submitting. Your 100 credits have been returned.",
        )
    for job in await store.active_songs():
        age_seconds = (datetime.now(timezone.utc) - job["created_at"]).total_seconds()
        if age_seconds > 65 * 60:
            await store.settle_song(
                job["id"], "failed", None,
                "This song expired before it could be retrieved. Your 100 credits have been returned.",
            )
            continue
        try:
            await sync_prediction(store, job, token, http_client, storage, persist=True)
        except Exception:
            pass  # Retry at next interval.
    if storage:
        await store.archive_legacy_songs(storage)


def create_app(
    *,
    token: str | None = None,
    store: Store | None = None,
    verify_google=verify_google_id_token,
    http_client: httpx.AsyncClient | None = None,
    support_email: str | None = None,
    storage=None,
    require_storage: bool = True,
    connect_database: bool = False,
) -> FastAPI:
    token = token if token is not None else os.environ.get("REPLICATE_API_TOKEN")
    support_email = support_email if support_email is not None else os.environ.get("CREDIT_SUPPORT_EMAIL", "")
    client = http_client or httpx.AsyncClient()
    allowed_origin = os.environ.get("ALLOWED_ORIGIN", "http://localhost:8080")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if connect_database:
            pool = await create_pool()
            await migrate(pool)
            app.state.pool = pool
            app.state.store = Store(pool)

            async def _loop():
                while True:
                    try:
                        await reconcile_pending(app.state.store, token, client, storage)
                    except Exception:
                        logging.exception("Prediction reconciliation failed")
                    await asyncio.sleep(15)

            app.state.reconcile_task = asyncio.create_task(_loop())
        yield
        if connect_database:
            if app.state.reconcile_task:
                app.state.reconcile_task.cancel()
            if app.state.pool:
                await app.state.pool.close()
            await client.aclose()

    app = FastAPI(lifespan=lifespan)
    app.state.store = store
    app.state.pool = None
    app.state.reconcile_task = None

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[allowed_origin],
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )

    @app.middleware("http")
    async def no_store(request: Request, call_next):
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        message = exc.detail if exc.status_code != 404 else "Not found."
        return JSONResponse(status_code=exc.status_code, content={"error": message})

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logging.exception("Request failed")
        return JSONResponse(status_code=500, content={"error": "The studio had a temporary problem. Please try again."})

    async def require_store(request: Request) -> Store:
        current = request.app.state.store
        if current is None:
            raise StarletteHTTPException(503, "The studio database is not connected. Set DATABASE_URL and run migrations.")
        return current

    async def get_bearer_token(authorization: str | None = Header(default=None)) -> str | None:
        match = _BEARER_RE.match(authorization or "")
        return match.group(1) if match else None

    async def get_authenticated_user(
        token: str | None = Depends(get_bearer_token),
        store: Store = Depends(require_store),
    ) -> dict:
        user = await store.user_for_token(token)
        if not user:
            raise StarletteHTTPException(401, "Please sign in with Google.")
        return user

    @app.get("/health")
    async def health():
        configured = bool(
            app.state.store and token and token != "replace_with_your_token" and os.environ.get("GOOGLE_WEB_CLIENT_ID")
        )
        return {"ok": True, "configured": configured, "storage_configured": bool(storage)}

    @app.post("/api/auth/google")
    async def auth_google(request: Request, store: Store = Depends(require_store)):
        try:
            body = await read_json_limited(request, 6000)
        except json.JSONDecodeError:
            return err(400, "Invalid JSON.")
        except ValueError as e:
            return err(400, str(e))
        id_token = body.get("id_token") if isinstance(body, dict) else None
        if not isinstance(id_token, str) or len(id_token) > 5000:
            return err(400, "A Google ID token is required.")
        try:
            identity = await verify_google(id_token)
            session = await store.sign_in_google(identity["sub"], identity["email"], identity["name"])
        except Exception:
            return err(401, "Google sign-in could not be verified.")
        return {"token": session["token"], "user": session["user"], "song_cost": SONG_COST, "support_email": support_email}

    @app.post("/api/auth/logout")
    async def logout(
        token: str | None = Depends(get_bearer_token),
        user: dict = Depends(get_authenticated_user),
        store: Store = Depends(require_store),
    ):
        await store.sign_out(token)
        return {"ok": True}

    @app.get("/api/me")
    async def me(user: dict = Depends(get_authenticated_user)):
        return {"user": user, "song_cost": SONG_COST, "support_email": support_email}

    @app.get("/api/credits")
    async def credits(user: dict = Depends(get_authenticated_user), store: Store = Depends(require_store)):
        return {"events": await store.credit_history(user["id"])}

    @app.post("/api/account/delete")
    async def account_delete(
        request: Request, user: dict = Depends(get_authenticated_user), store: Store = Depends(require_store)
    ):
        try:
            body = await read_json_limited(request)
        except (json.JSONDecodeError, ValueError):
            return err(400, "Confirm account deletion.")
        if not isinstance(body, dict) or body.get("confirmation") != "DELETE":
            return err(400, "Confirm account deletion.")
        try:
            await store.delete_account(user["id"], storage)
        except Exception as e:
            return err(409, str(e))
        return {"ok": True}

    @app.get("/api/songs")
    async def list_songs(user: dict = Depends(get_authenticated_user), store: Store = Depends(require_store)):
        rows = await store.list_songs(user["id"])
        return {"songs": [await present_song(row, storage) for row in rows]}

    @app.post("/api/songs")
    async def create_song(
        request: Request, user: dict = Depends(get_authenticated_user), store: Store = Depends(require_store)
    ):
        if not token or token == "replace_with_your_token":
            return err(503, "The music model is not connected. Set REPLICATE_API_TOKEN.")
        if require_storage and not storage:
            return err(503, "Song storage is not ready. Please try again later. No credits were charged.")

        try:
            body = await read_json_limited(request)
            song_input = validate_input(body)
            request_id = body.get("request_id")
            if request_id is not None and not _is_request_id(request_id):
                raise ValueError("Invalid request ID.")
            settings = {
                **song_input,
                "genre": str(body.get("genre") or "")[:80],
                "mood": str(body.get("mood") or "")[:80],
                "vocals": str(body.get("vocals") or "")[:80],
                "bpm": int(max(40, min(220, _js_number_or(body.get("bpm"), 100)))),
                "direction": str(body.get("direction") or "")[:2000],
            }
            title_raw, style_raw = body.get("title"), body.get("style")
            title = title_raw.strip()[:80] if isinstance(title_raw, str) else ""
            style = style_raw.strip()[:120] if isinstance(style_raw, str) else ""
        except json.JSONDecodeError:
            return err(400, "Invalid JSON.")
        except ValueError as e:
            return err(400, str(e))

        try:
            reservation = await store.reserve_song(
                user["id"], title or "Untitled session", style or "Custom sound", settings, request_id
            )
        except InsufficientCredits as e:
            current = await store.get_user(user["id"])
            return err(402, str(e), credits=(current or {}).get("credits", 0), song_cost=SONG_COST)
        except Exception:
            return err(500, "Could not reserve credits. Please try again.")

        if reservation["existing"]:
            song = await store.get_song(user["id"], reservation["id"])
            presented = await present_song(song, storage)
            current = await store.get_user(user["id"])
            return {**presented, "credits": current["credits"]}

        try:
            upstream = await client.post(
                "https://api.replicate.com/v1/models/minimax/music-2.6/predictions",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"input": song_input},
                timeout=35.0,
            )
            if upstream.status_code >= 400:
                raise RuntimeError("Replicate rejected the song request. Your 100 credits have been returned.")
            data = upstream.json()
            if not data.get("id"):
                raise RuntimeError("The music service returned no song ID. Your 100 credits have been returned.")
            await store.attach_prediction(reservation["id"], data)
            if data.get("status") in ("succeeded", "failed", "canceled"):
                current_song = await store.get_song(user["id"], reservation["id"])
                await finish_prediction(store, current_song, data, storage)
            song = await store.get_song(user["id"], reservation["id"])
            presented = await present_song(song, storage)
            current = await store.get_user(user["id"])
            return {**presented, "credits": current["credits"]}
        except Exception as e:
            await store.settle_song(reservation["id"], "failed", None, str(e))
            return err(502, str(e))

    @app.get("/api/songs/{song_id}")
    async def get_song_route(
        song_id: str, user: dict = Depends(get_authenticated_user), store: Store = Depends(require_store)
    ):
        if not _SONG_ID_RE.match(song_id):
            return err(404, "Not found.")
        song = await store.get_song(user["id"], song_id)
        if not song:
            return err(404, "Song not found.")
        try:
            song = await sync_prediction(store, song, token, client, storage)
        except Exception:
            pass  # Background retry will settle it.
        if not song:
            return err(404, "Song not found.")
        presented = await present_song(song, storage)
        current = await store.get_user(user["id"])
        return {**presented, "credits": current["credits"]}

    @app.get("/api/songs/{song_id}/download")
    async def download_song(
        song_id: str, user: dict = Depends(get_authenticated_user), store: Store = Depends(require_store)
    ):
        if not _SONG_ID_RE.match(song_id):
            return err(404, "Not found.")
        song = await store.get_song(user["id"], song_id)
        if not song:
            return err(404, "Song not found.")
        if not song.get("storage_key") or not storage:
            return err(409, "This audio is not saved yet. Please try again later.")
        return {"url": await storage.url(song["storage_key"], True)}

    return app


if __name__ == "__main__":
    import uvicorn

    production_app = create_app(
        token=os.environ.get("REPLICATE_API_TOKEN"),
        storage=create_audio_storage(),
        connect_database=True,
    )
    uvicorn.run(
        production_app,
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8787")),
    )
