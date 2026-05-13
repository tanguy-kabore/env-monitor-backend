import os
import yaml
from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Any


class Settings(BaseSettings):
    supabase_url: str = ""
    supabase_key: str = ""
    config_path: str = ""

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


class AppConfig:
    _instance = None
    _config: dict = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def load(self, config_path: str = None):
        if config_path is None:
            env_path = get_settings().config_path
            if env_path:
                config_path = env_path
            else:
                config_path = "config/app_config.yaml"
        path = Path(config_path)
        if not path.is_absolute():
            backend_root = Path(__file__).parent.parent
            path = backend_root / config_path
        with open(path, "r", encoding="utf-8") as f:
            self._config = yaml.safe_load(f)
        return self

    @property
    def config(self) -> dict:
        if not self._config:
            self.load()
        return self._config

    def get(self, key: str, default: Any = None) -> Any:
        keys = key.split(".")
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value

    @property
    def app_name(self) -> str:
        return self.get("app.name", "EcoWatch")

    @property
    def app_version(self) -> str:
        return self.get("app.version", "1.0.0")

    @property
    def country(self) -> dict:
        return self.get("country", {})

    @property
    def regions(self) -> list:
        return self.get("regions", [])

    @property
    def cities(self) -> list:
        return self.get("cities", [])

    @property
    def apis(self) -> dict:
        return self.get("apis", {})

    @property
    def data_collection(self) -> dict:
        return self.get("data_collection", {})

    @property
    def ml_config(self) -> dict:
        return self.get("ml", {})

    @property
    def alert_thresholds(self) -> dict:
        return self.get("alerts.thresholds", {})

    def get_all_locations(self) -> list:
        locations = []
        for region in self.regions:
            locations.append({
                "external_id": region["id"],
                "name": region["name"],
                "type": "region",
                "latitude": region["latitude"],
                "longitude": region["longitude"],
                "parent_id": None,
            })
            for province in region.get("provinces", []):
                locations.append({
                    "external_id": province["id"],
                    "name": province["name"],
                    "type": "province",
                    "latitude": province["latitude"],
                    "longitude": province["longitude"],
                    "parent_external_id": region["id"],
                })
        for city in self.cities:
            locations.append({
                "external_id": city["id"],
                "name": city["name"],
                "type": "city",
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "population": city.get("population"),
                "parent_external_id": city.get("province_id"),
            })
            for q in city.get("quartiers", []):
                locations.append({
                    "external_id": q["id"],
                    "name": q["name"],
                    "type": "quartier",
                    "latitude": q["latitude"],
                    "longitude": q["longitude"],
                    "parent_external_id": city["id"],
                })
        return locations

    def get_monitorable_locations(self) -> list:
        locs = []
        for city in self.cities:
            locs.append({
                "external_id": city["id"],
                "name": city["name"],
                "latitude": city["latitude"],
                "longitude": city["longitude"],
            })
        return locs


@lru_cache()
def get_app_config() -> AppConfig:
    config = AppConfig()
    config.load()
    return config
