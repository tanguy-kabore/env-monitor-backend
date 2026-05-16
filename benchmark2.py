"""
Focused benchmark: test only the key endpoints, with pauses to avoid rate limits.
"""
import asyncio, sys, time
import httpx

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


async def get(client, url, label):
    t0 = time.perf_counter()
    try:
        r = await client.get(url, timeout=90)
        ms = (time.perf_counter() - t0) * 1000
        tag = "SLOW " if ms > 2000 else ("OK   " if ms > 500 else "FAST ")
        print(f"  [{tag}] {label:<28} {ms:>7.0f}ms  [{r.status_code}]")
        return r.status_code
    except Exception as e:
        print(f"  [ERROR] {label:<28} {type(e).__name__}")
        return None


async def main():
    async with httpx.AsyncClient(base_url="http://localhost:8000") as c:
        # Discover a city
        r = await c.get("/api/v1/locations/cities", timeout=30)
        city = r.json()["data"][0]["external_id"] if r.status_code == 200 else "ouagadougou"
        print(f"\nCity: {city}")
        print("=" * 65)

        print("\n[COLD HITS - first visit]\n")
        await get(c, "/api/v1/dashboard", "Dashboard")
        await asyncio.sleep(1)
        await get(c, "/api/v1/weather/summary", "Weather summary")
        await asyncio.sleep(1)
        await get(c, "/api/v1/floods/risk-map", "Flood risk map")
        await asyncio.sleep(1)
        await get(c, "/api/v1/air-quality/map", "AQ map")
        await asyncio.sleep(1)
        await get(c, "/api/v1/drought/map", "Drought map")
        await asyncio.sleep(1)
        await get(c, "/api/v1/locations/cities", "Cities list")
        await asyncio.sleep(1)
        await get(c, "/api/v1/locations/regions", "Regions list")
        await asyncio.sleep(1)
        await get(c, "/api/v1/alerts/stats", "Alert stats")
        await asyncio.sleep(1)
        await get(c, f"/api/v1/weather/current/{city}", "Weather current")
        await asyncio.sleep(1)
        await get(c, f"/api/v1/floods/current/{city}", "Flood current")
        await asyncio.sleep(1)
        await get(c, f"/api/v1/air-quality/current/{city}", "AQ current")
        await asyncio.sleep(1)
        await get(c, f"/api/v1/drought/current/{city}", "Drought current")

        print("\n[WARM HITS - should be <20ms from cache]\n")
        await asyncio.sleep(2)
        await get(c, "/api/v1/dashboard", "Dashboard")
        await get(c, "/api/v1/weather/summary", "Weather summary")
        await get(c, "/api/v1/floods/risk-map", "Flood risk map")
        await get(c, "/api/v1/air-quality/map", "AQ map")
        await get(c, "/api/v1/drought/map", "Drought map")
        await get(c, "/api/v1/locations/cities", "Cities list")
        await get(c, "/api/v1/locations/regions", "Regions list")
        await get(c, "/api/v1/alerts/stats", "Alert stats")
        await get(c, f"/api/v1/weather/current/{city}", "Weather current")
        await get(c, f"/api/v1/floods/current/{city}", "Flood current")
        await get(c, f"/api/v1/air-quality/current/{city}", "AQ current")
        await get(c, f"/api/v1/drought/current/{city}", "Drought current")

asyncio.run(main())
