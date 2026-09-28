import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectsCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

export function createAudioStorage(env = process.env) {
  const { R2_ACCOUNT_ID, R2_BUCKET_NAME, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY } = env;
  if (![R2_ACCOUNT_ID, R2_BUCKET_NAME, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY].every(Boolean)) return null;
  const client = new S3Client({ region: 'auto', endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY },
    requestChecksumCalculation: 'WHEN_REQUIRED', responseChecksumValidation: 'WHEN_REQUIRED', maxAttempts: 3 });
  const Bucket = R2_BUCKET_NAME;
  return {
    async save(song, source) {
      const url = new URL(source);
      if (url.protocol !== 'https:' || !(url.hostname === 'replicate.delivery' || url.hostname.endsWith('.replicate.delivery'))) {
        throw new Error('Unsupported audio source.');
      }
      const response = await fetch(url, { signal: AbortSignal.timeout(120000), redirect: 'error' });
      if (!response.ok || !response.body) throw new Error('Audio is not available yet.');
      const limit = 64 * 1024 * 1024;
      const chunks = []; let size = 0;
      for await (const chunk of response.body) {
        size += chunk.length;
        if (size > limit) throw new Error('Audio exceeds the storage limit.');
        chunks.push(chunk);
      }
      if (!size) throw new Error('Audio file is empty.');
      const Key = `users/${song.user_id}/songs/${song.id}.mp3`;
      await client.send(new PutObjectCommand({ Bucket, Key, Body: Buffer.concat(chunks), ContentType: 'audio/mpeg' }),
        { abortSignal: AbortSignal.timeout(120000) });
      return Key;
    },
    async url(key, download = false) {
      return getSignedUrl(client, new GetObjectCommand({ Bucket, Key: key,
        ResponseContentDisposition: download ? 'attachment; filename="verse-song.mp3"' : 'inline' }), { expiresIn: 3600 });
    },
    async deleteUser(userId) {
      // Start from the prefix again after each batch because deleted keys change pagination.
      for (;;) {
        const page = await client.send(new ListObjectsV2Command({ Bucket, Prefix: `users/${userId}/`, MaxKeys: 1000 }));
        if (!page.Contents?.length) return;
        const deleted = await client.send(new DeleteObjectsCommand({ Bucket,
          Delete: { Objects: page.Contents.map(({ Key }) => ({ Key })) } }));
        if (deleted.Errors?.length) throw new Error('Some audio files could not be deleted. Please retry.');
      }
    },
  };
}
