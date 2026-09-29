import inspect
import os
from urllib.parse import urlparse

import aioboto3
import httpx
from botocore.config import Config

_REPLICATE_SUFFIX = ".replicate.delivery"
_DOWNLOAD_LIMIT = 64 * 1024 * 1024


def create_audio_storage(env=None):
    env = env if env is not None else os.environ
    account_id = env.get("R2_ACCOUNT_ID")
    bucket_name = env.get("R2_BUCKET_NAME")
    access_key_id = env.get("R2_ACCESS_KEY_ID")
    secret_access_key = env.get("R2_SECRET_ACCESS_KEY")
    if not all([account_id, bucket_name, access_key_id, secret_access_key]):
        return None
    client_kwargs = dict(
        service_name="s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        region_name="auto",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
    )
    return AudioStorage(aioboto3.Session(), client_kwargs, bucket_name)


class AudioStorage:
    def __init__(self, session, client_kwargs, bucket):
        self._session = session
        self._client_kwargs = client_kwargs
        self._bucket = bucket

    def _client(self):
        return self._session.client(**self._client_kwargs)

    async def save(self, song: dict, source: str) -> str:
        url = urlparse(source)
        host = url.hostname or ""
        if url.scheme != "https" or not (host == "replicate.delivery" or host.endswith(_REPLICATE_SUFFIX)):
            raise ValueError("Unsupported audio source.")
        async with httpx.AsyncClient(follow_redirects=False, timeout=120.0) as http:
            async with http.stream("GET", source) as response:
                if not (200 <= response.status_code < 300):
                    raise ValueError("Audio is not available yet.")
                chunks = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > _DOWNLOAD_LIMIT:
                        raise ValueError("Audio exceeds the storage limit.")
                    chunks.append(chunk)
        if not size:
            raise ValueError("Audio file is empty.")
        key = f"users/{song['user_id']}/songs/{song['id']}.mp3"
        async with self._client() as client:
            await client.put_object(
                Bucket=self._bucket, Key=key, Body=b"".join(chunks), ContentType="audio/mpeg"
            )
        return key

    async def url(self, key: str, download: bool = False) -> str:
        params = {
            "Bucket": self._bucket,
            "Key": key,
            "ResponseContentDisposition": 'attachment; filename="verse-song.mp3"' if download else "inline",
        }
        async with self._client() as client:
            result = client.generate_presigned_url("get_object", Params=params, ExpiresIn=3600)
            if inspect.isawaitable(result):
                result = await result
            return result

    async def delete_user(self, user_id) -> None:
        prefix = f"users/{user_id}/"
        async with self._client() as client:
            while True:
                page = await client.list_objects_v2(Bucket=self._bucket, Prefix=prefix, MaxKeys=1000)
                contents = page.get("Contents") or []
                if not contents:
                    return
                deleted = await client.delete_objects(
                    Bucket=self._bucket,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in contents]},
                )
                if deleted.get("Errors"):
                    raise RuntimeError("Some audio files could not be deleted. Please retry.")
