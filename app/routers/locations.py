from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase, db_exec
from app.config import get_app_config
from app import cache as _cache
from typing import Optional

router = APIRouter(prefix="/api/v1/locations", tags=["Locations"])

_LOC_FIELDS = "id,external_id,name,type,latitude,longitude,parent_id,population,is_active"


@router.get("")
async def get_locations(
    type: Optional[str] = Query(None),
    parent_id: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    fields: Optional[str] = Query(None),
):
    select_fields = fields if fields else _LOC_FIELDS
    client = get_supabase()

    def _fetch():
        q = client.table("locations").select(select_fields, count="exact")
        if type:
            q = q.eq("type", type)
        if parent_id:
            q = q.eq("parent_id", parent_id)
        if search:
            q = q.ilike("name", f"%{search}%")
        return q.order("population", desc=True).range(offset, offset + limit - 1).execute()

    result = await db_exec(_fetch)
    return {
        "data": result.data,
        "count": len(result.data),
        "total": result.count,
        "limit": limit,
        "offset": offset,
    }


@router.get("/regions")
async def get_regions():
    cache_key = "locations:regions"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    result = await db_exec(lambda: client.table("locations")
        .select(_LOC_FIELDS).eq("type", "region").order("name").limit(50).execute())
    response = {"data": result.data}
    _cache.set(cache_key, response, ttl=3_600)
    return response


@router.get("/cities")
async def get_cities():
    cache_key = "locations:cities"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    client = get_supabase()
    result = await db_exec(lambda: client.table("locations")
        .select(_LOC_FIELDS).eq("type", "city").order("name").limit(300).execute())
    response = {"data": result.data}
    _cache.set(cache_key, response, ttl=3_600)
    return response


@router.get("/tree")
async def get_location_tree():
    config = get_app_config()
    return {
        "country": config.country,
        "regions": config.regions,
        "cities": config.cities,
    }


@router.get("/{location_id}")
async def get_location(location_id: str):
    client = get_supabase()
    result = await db_exec(lambda: client.table("locations")
        .select(_LOC_FIELDS).eq("id", location_id).limit(1).execute())
    if not result.data:
        result = await db_exec(lambda: client.table("locations")
            .select(_LOC_FIELDS).eq("external_id", location_id).limit(1).execute())
    if not result.data:
        raise HTTPException(status_code=404, detail="Location not found")
    return {"data": result.data[0]}


@router.get("/{location_id}/children")
async def get_children(location_id: str):
    client = get_supabase()
    loc = await db_exec(lambda: client.table("locations")
        .select("id").eq("external_id", location_id).limit(1).execute())
    uuid = loc.data[0]["id"] if loc.data else location_id

    result = await db_exec(lambda: client.table("locations")
        .select(_LOC_FIELDS).eq("parent_id", uuid).order("name").limit(200).execute())
    return {"data": result.data}
