"""Content and identity hashing."""

from __future__ import annotations

import hashlib

_CHUNK = 1024 * 1024


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_stream(chunks) -> tuple[str, int]:
    """Hash an iterable of byte chunks without holding the whole body in memory.

    Returns (hex_digest, total_bytes).
    """
    digest = hashlib.sha256()
    size = 0
    for chunk in chunks:
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def shard_index(key: str | int, shard_count: int) -> int:
    """Map a key to a shard, identically in every process.

    Python salts str.__hash__ per process (PYTHONHASHSEED), so the builtin
    hash() assigns the same key to different shards in different Cloud Run
    tasks — work gets both duplicated and dropped. A digest is stable
    everywhere, which is what makes parallel tasks partition cleanly.
    """
    if shard_count <= 1:
        return 0
    digest = hashlib.blake2b(str(key).encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % shard_count


def doc_id(url: str) -> str:
    """Stable Firestore document id for a URL.

    Firestore ids can't contain '/', and raw URLs are unbounded in length, so we
    key on a digest. Same URL always maps to the same document, which is what
    makes re-running a crawl idempotent rather than duplicative.
    """
    return hashlib.sha1(url.encode("utf-8")).hexdigest()
