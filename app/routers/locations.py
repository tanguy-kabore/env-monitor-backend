from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase
from app.config import get_app_config
from typing import Optional

router = APIRouter(prefix="/api/locations", tags=["Locations"])


@router.get("")
async def get_locations(
    type: Optional[str] = Query(None, description="Filter by type: region, province, city, quartier"),
    parent_id: Optional[str] = Query(None, description="Filter by parent location UUID"),
    search: Optional[str] = Query(None, description="Search by name"),
):
    client = get_supabase()
    query = client.table("locations").select("*")

    if type:
        query = query.eq("type", type)
    if parent_id:
        query = query.eq("parent_id", parent_id)
    if search:
        query = query.ilike("name", f"%{search}%")

    result = query.order("name").execute()
    return {"data": result.data, "count": len(result.data)}


@router.get("/regions")
async def get_regions():
    client = get_supabase()
    result = (
        client.table("locations")
        .select("*")
        .eq("type", "region")
        .order("name")
        .execute()
    )
    return {"data": result.data}


@router.get("/cities")
async def get_cities():
    client = get_supabase()
    result = (
        client.table("locations")
        .select("*")
        .eq("type", "city")
        .order("name")
        .execute()
    )
    return {"data": result.data}


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
    result = (
        client.table("locations")
        .select("*")
        .eq("id", location_id)
        .execute()
    )
    if not result.data:
        result = (
            client.table("locations")
            .select("*")
            .eq("external_id", location_id)
            .execute()
        )
    if not result.data:
        raise HTTPException(status_code=404, detail="Location not found")
    return {"data": result.data[0]}


@router.get("/{location_id}/children")
async def get_children(location_id: str):
    client = get_supabase()
    loc = (
        client.table("locations")
        .select("id")
        .eq("external_id", location_id)
        .execute()
    )
    uuid = loc.data[0]["id"] if loc.data else location_id

    result = (
        client.table("locations")
        .select("*")
        .eq("parent_id", uuid)
        .order("name")
        .execute()
    )
    return {"data": result.data}
