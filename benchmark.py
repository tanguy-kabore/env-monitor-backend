"""
Benchmark: measures cold and warm response times for key endpoints.
Run: python benchmark.py
"""
import asyncio
import sys
import time
import httpx

# Force UTF-8 output on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ENDPOINTS = [
    ("Dashboard",          "/api/v1/dashboard"),
    ("Weather summary",    "/api/v1/weather/summary"),
    ("Flood risk map",     "/api/v1/floods/risk-map"),
    ("AQ map",             "/api/v1/air-quality/map"),
    ("Drought map",        "/api/v1/drought/map"),
    ("Cities list",        "/api/v1/locations/cities"),
    ("Regions list",       "/api/v1/locations/regions"),
    ("Alert stats",        "/api/v1/alerts/stats"),
]

CITY_ENDPOINTS = [
    ("Weather current",    "/api/v1/weather/current/{city}"),
    ("Weather forecast",   "/api/v1/weather/forecast/{city}"),
    ("Weather history30",  "/api/v1/weather/history/{city}?days=30"),
    ("Flood current",      "/api/v1/floods/current/{city}"),
    ("Flood forecast",     "/api/v1/floods/forecast/{city}"),
    ("Flood history30",    "/api/v1/floods/history/{city}?days=30"),
    ("AQ current",         "/api/v1/air-quality/current/{city}"),
    ("AQ forecast",        "/api/v1/air-quality/forecast/{city}"),
    ("Drought current",    "/api/v1/drought/current/{city}"),
    ("Climate trends",     "/api/v1/climate/trends/{city}"),
    ("Location detail",    "/api/v1/locations/{city}"),
    ("Alerts by loc",      "/api/v1/alerts/location/{city}"),
]


async def measure(client, label, url, runs=3):
    times = []
    status = None
    for _ in range(runs):
        t0 = time.perf_counter()
        try:
            r = await client.get(url, timeout=90)
            status = r.status_code
        except Exception as e:
            print(f"  ERROR  {label:<25} {type(e).__name__}: {e}")
            return
        times.append((time.perf_counter() - t0) * 1000)

    cold = times[0]
    warm_avg = sum(times[1:]) / max(len(times) - 1, 1)
    tag = "SLOW " if cold > 2000 else ("OK   " if cold > 500 else "FAST ")
    note = "cached" if warm_avg < 20 else f"warm={warm_avg:.0f}ms"
    print(f"  [{tag}] {label:<25} cold={cold:>7.0f}ms  {note}  [{status}]")


async def main():
    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        r = await client.get("/api/v1/locations/cities", timeout=30)
        cities = r.json().get("data", [])
        city = cities[0]["external_id"] if cities else "ouagadougou"
        print(f"\nBenchmarking city: {city}")
        print("=" * 72)

        print("\n--- Global endpoints (cold + 2 warm) ---\n")
        for label, path in ENDPOINTS:
            await measure(client, label, path)

        print(f"\n--- Per-city endpoints (city={city}) ---\n")
        for label, path in CITY_ENDPOINTS:
            await measure(client, label, path.replace("{city}", city))

        print("\n--- Cache check (global, single hit, should be <15ms) ---\n")
        for label, path in ENDPOINTS:
            await measure(client, label, path, runs=1)


if __name__ == "__main__":
    asyncio.run(main())
