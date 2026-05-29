import asyncio
import logging
from supabase import create_client, Client
from app.config import get_settings
from app import cache as _cache

logger = logging.getLogger(__name__)


async def db_exec(query_fn):
    """Run a synchronous Supabase query builder in a thread so it never blocks the event loop."""
    return await asyncio.to_thread(query_fn)


async def resolve_location_uuid_async(location_id: str) -> str:
    """Async-safe wrapper around resolve_location_uuid — won't block the event loop."""
    return await asyncio.to_thread(resolve_location_uuid, location_id)

_client: Client = None


def get_supabase() -> Client:
    global _client
    if _client is None:
        settings = get_settings()
        if not settings.supabase_url or not settings.supabase_key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY must be set in .env file"
            )
        _client = create_client(settings.supabase_url, settings.supabase_key)
        logger.info("Supabase client initialized")
    return _client


def check_connection() -> bool:
    try:
        client = get_supabase()
        client.table("system_config").select("key").limit(1).execute()
        return True
    except Exception as e:
        logger.error(f"Supabase connection check failed: {e}")
        return False


async def upsert_locations(locations: list) -> int:
    client = get_supabase()
    inserted = 0
    id_map = {}

    for loc in locations:
        existing = (
            client.table("locations")
            .select("id")
            .eq("external_id", loc["external_id"])
            .execute()
        )
        if existing.data:
            id_map[loc["external_id"]] = existing.data[0]["id"]
            continue

        parent_uuid = None
        parent_ext = loc.get("parent_external_id")
        if parent_ext and parent_ext in id_map:
            parent_uuid = id_map[parent_ext]
        elif parent_ext:
            parent_result = (
                client.table("locations")
                .select("id")
                .eq("external_id", parent_ext)
                .execute()
            )
            if parent_result.data:
                parent_uuid = parent_result.data[0]["id"]
                id_map[parent_ext] = parent_uuid

        record = {
            "external_id": loc["external_id"],
            "name": loc["name"],
            "type": loc["type"],
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
            "parent_id": parent_uuid,
        }
        if loc.get("population"):
            record["population"] = loc["population"]

        result = client.table("locations").insert(record).execute()
        if result.data:
            id_map[loc["external_id"]] = result.data[0]["id"]
            inserted += 1

    return inserted


def get_location_uuid(external_id: str) -> str:
    cache_key = f"loc_uuid:{external_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    client = get_supabase()
    result = (
        client.table("locations")
        .select("id")
        .eq("external_id", external_id)
        .limit(1)
        .execute()
    )
    if result.data:
        uuid = result.data[0]["id"]
        _cache.set(cache_key, uuid, ttl=3600)
        return uuid
    return None


def resolve_location_uuid(location_id: str) -> str:
    import re
    uuid_pattern = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE
    )
    if uuid_pattern.match(location_id):
        return location_id
    uuid = get_location_uuid(location_id)
    if uuid is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Location '{location_id}' not found")
    return uuid


async def prime_location_cache() -> int:
    """Bulk-load all location UUIDs into the cache at startup so every
    resolve_location_uuid call is a dictionary hit rather than a DB round-trip."""
    mapping = await asyncio.to_thread(get_all_location_uuids)
    for ext_id, uuid in mapping.items():
        _cache.set(f"loc_uuid:{ext_id}", uuid, ttl=3600)
    logger.info("Location UUID cache primed: %d entries", len(mapping))
    return len(mapping)


def get_all_location_uuids() -> dict:
    cache_key = "all_location_uuids"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    client = get_supabase()
    result = client.table("locations").select("id, external_id").execute()
    mapping = {row["external_id"]: row["id"] for row in result.data}
    _cache.set(cache_key, mapping, ttl=3600)
    return mapping


UPSERT_CONFLICT_COLS = {
    "weather_data": "location_id,observed_at",
    "flood_data": "location_id,observed_at",
    "air_quality_data": "location_id,observed_at",
    "drought_data": "location_id,observed_at",
    "climate_data": "location_id,year,month",
}


def insert_batch(table: str, records: list) -> int:
    if not records:
        return 0
    client = get_supabase()
    batch_size = 500
    total = 0
    failed_batches = 0
    conflict_cols = UPSERT_CONFLICT_COLS.get(table)
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        try:
            if conflict_cols:
                result = client.table(table).upsert(batch, on_conflict=conflict_cols).execute()
            else:
                result = client.table(table).insert(batch).execute()
            total += len(result.data) if result.data else 0
        except Exception as e:
            failed_batches += 1
            logger.error(f"Batch insert error on {table} (batch {i // batch_size + 1}): {e}")
    if failed_batches:
        logger.warning("insert_batch(%s): %d batch(es) failed — %d records inserted out of %d attempted",
                       table, failed_batches, total, len(records))
    return total


def get_system_config(key: str):
    client = get_supabase()
    result = (
        client.table("system_config")
        .select("value")
        .eq("key", key)
        .execute()
    )
    if result.data:
        return result.data[0]["value"]
    return None


def set_system_config(key: str, value):
    client = get_supabase()
    if isinstance(value, bool):
        str_value = "true" if value else "false"
    elif value is None:
        str_value = ""
    else:
        str_value = str(value)
    try:
        client.table("system_config").upsert(
            {"key": key, "value": str_value},
            on_conflict="key"
        ).execute()
    except Exception:
        client.table("system_config").update(
            {"value": str_value}
        ).eq("key", key).execute()
