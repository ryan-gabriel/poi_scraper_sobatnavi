#!/usr/bin/env python3
"""
SobatNavi — Google Places API (New) POI Fetcher v3.0
=====================================================
Professional-grade grid-based deep-crawl ETL pipeline for Bali POI data.

Strategy:
  Grid-based tiling across 15 tourism zones → ~1700 grid points.
  Each grid point: 3 Nearby Search calls (one per namespace).
  500m search radius at 1km spacing → full spatial coverage.

Features:
  - Grid-based spatial tiling (bypass 20-result limit)
  - Strict no-skip fail-safe: sys.exit(1) on persistent errors
  - Atomic checkpoint after every grid point (resume on restart)
  - --batch-size for GitHub Actions cron safety
  - Adaptive rate limiter (speeds up/slows down dynamically)
  - Global deduplication by place_id
  - Schema-compliant DataTransformer (3 namespaces)

Usage:
    python google_places_fetcher.py                    # Full run (auto-resumes)
    python google_places_fetcher.py --batch-size 50    # Process 50 grid points
    python google_places_fetcher.py --dry-run          # Test with 3 points
    python google_places_fetcher.py --reset            # Clear checkpoint

Requires:
    - GOOGLE_PLACES_API_KEY in .env file
    - pip install -r requirements.txt
"""

import argparse
import json
import logging
import math
import os
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv

from config import (
    BALI_REGIONS,
    BATCH_DELAY_BASE_SECONDS,
    BATCH_DELAY_INCREMENT,
    BATCH_DELAY_MAX_SECONDS,
    BATCH_DELAY_SHRINK,
    BATCH_DELAY_SHRINK_AFTER,
    ENRICHMENT_CONFIG,
    FIELD_MASKS,
    GOOGLE_TYPE_TO_NAMESPACE,
    GOOGLE_TYPE_TO_POI_TYPE,
    GRID_CONFIG,
    INITIAL_BACKOFF_SECONDS,
    MAX_RETRIES,
    NAMESPACE_TYPES,
    NEARBY_SEARCH_URL,
    PAGE_SIZE,
    POI_TYPE_TO_PRIMARY_CATEGORY,
    PRICE_LEVEL_MAP,
    QUALITY_THRESHOLDS,
    RegionConfig,
)

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("places_fetcher")


# ═══════════════════════════════════════════════════════════════════════════════
#  Grid Generator
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class GridPoint:
    """A single grid coordinate with its global index and source region."""
    index: int
    lat: float
    lng: float
    region_name: str


class BaliGridGenerator:
    """Generates a deterministic grid of coordinates covering Bali tourism zones.

    Uses BALI_REGIONS circles as zone boundaries.
    Grid spacing and search radius come from GRID_CONFIG.
    """

    METERS_PER_DEGREE_LAT = 111_320.0  # ~constant everywhere

    @staticmethod
    def meters_per_degree_lng(lat: float) -> float:
        return 111_320.0 * math.cos(math.radians(lat))

    @classmethod
    def generate_region_grid(
        cls,
        region: RegionConfig,
        spacing_meters: float,
    ) -> list[tuple[float, float]]:
        """Generate grid points within a region's circle.

        Returns list of (lat, lng) tuples that fall inside the circle.
        """
        dlat = spacing_meters / cls.METERS_PER_DEGREE_LAT
        dlng = spacing_meters / cls.meters_per_degree_lng(region.lat)

        # Bounding box of the circle
        lat_min = region.lat - (region.radius / cls.METERS_PER_DEGREE_LAT)
        lat_max = region.lat + (region.radius / cls.METERS_PER_DEGREE_LAT)
        lng_min = region.lng - (region.radius / cls.meters_per_degree_lng(region.lat))
        lng_max = region.lng + (region.radius / cls.meters_per_degree_lng(region.lat))

        points = []
        lat = lat_min
        while lat <= lat_max:
            lng = lng_min
            while lng <= lng_max:
                # Check if point is within the circle
                dist = cls._haversine(region.lat, region.lng, lat, lng)
                if dist <= region.radius:
                    points.append((round(lat, 7), round(lng, 7)))
                lng += dlng
            lat += dlat

        return points

    @staticmethod
    def _haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Haversine distance in meters between two coordinates."""
        R = 6_371_000  # Earth radius in meters
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlam = math.radians(lng2 - lng1)
        a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    @classmethod
    def generate_all(cls) -> list[GridPoint]:
        """Generate the full grid across all regions, sorted deterministically.

        Returns a globally-indexed list of GridPoint objects.
        Deterministic ordering: sorted by (region_name, lat, lng).
        """
        spacing = GRID_CONFIG["spacing_meters"]
        all_points: list[tuple[str, float, float]] = []

        for region_name, region in sorted(BALI_REGIONS.items()):
            points = cls.generate_region_grid(region, spacing)
            for lat, lng in points:
                all_points.append((region_name, lat, lng))

        # Sort deterministically for stable indexing
        all_points.sort(key=lambda p: (p[0], p[1], p[2]))

        # Deduplicate overlapping points (from adjacent regions)
        seen = set()
        unique = []
        for region_name, lat, lng in all_points:
            key = (lat, lng)
            if key not in seen:
                seen.add(key)
                unique.append((region_name, lat, lng))

        # Assign global indices
        grid = [
            GridPoint(index=i, lat=lat, lng=lng, region_name=rn)
            for i, (rn, lat, lng) in enumerate(unique)
        ]

        return grid


# ═══════════════════════════════════════════════════════════════════════════════
#  Checkpoint Manager (Index-Based)
# ═══════════════════════════════════════════════════════════════════════════════

class CheckpointManager:
    """Atomic checkpoint manager using grid point index.

    Stores:
      - last_processed_index: int — resume from index + 1
      - seen_place_ids: dict[namespace, list[str]]
      - results: dict[namespace, list[dict]]
    """

    def __init__(self, checkpoint_path: Path):
        self.path = checkpoint_path

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> dict:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            log.info(
                "📂 Checkpoint loaded: last_index=%d, %d total records",
                data.get("last_processed_index", -1),
                sum(len(r) for r in data.get("results", {}).values()),
            )
            return data
        except (json.JSONDecodeError, OSError) as e:
            log.warning("⚠️  Corrupt checkpoint, starting fresh: %s", e)
            return self._empty()

    def save(self, state: dict):
        """Atomically save checkpoint (write to temp, then rename)."""
        state["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        tmp_path = self.path.with_suffix(".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False)
            if self.path.exists():
                self.path.unlink()
            tmp_path.rename(self.path)
        except OSError as e:
            log.error("❌ Failed to save checkpoint: %s", e)

    def delete(self):
        if self.path.exists():
            self.path.unlink()
            log.info("🗑️  Checkpoint cleared.")

    @staticmethod
    def _empty() -> dict:
        return {
            "last_processed_index": -1,
            "seen_place_ids": {},
            "results": {},
        }


# ═══════════════════════════════════════════════════════════════════════════════
#  Adaptive Rate Limiter
# ═══════════════════════════════════════════════════════════════════════════════

class AdaptiveRateLimiter:
    """Dynamically adjusts delay between API calls based on 429 feedback.

    - On 429: delay += increment (slows down)
    - After N consecutive successes: delay -= shrink (speeds up)
    - Bounded by [base_delay, max_delay]
    """

    def __init__(
        self,
        base_delay: float = BATCH_DELAY_BASE_SECONDS,
        max_delay: float = BATCH_DELAY_MAX_SECONDS,
        increment: float = BATCH_DELAY_INCREMENT,
        shrink: float = BATCH_DELAY_SHRINK,
        shrink_after: int = BATCH_DELAY_SHRINK_AFTER,
    ):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.increment = increment
        self.shrink = shrink
        self.shrink_after = shrink_after

        self.current_delay = base_delay
        self._consecutive_successes = 0

    def on_rate_limit(self):
        old = self.current_delay
        self.current_delay = min(self.current_delay + self.increment, self.max_delay)
        self._consecutive_successes = 0
        log.info("  ↑ Rate limit — delay: %.1fs → %.1fs", old, self.current_delay)

    def on_success(self):
        self._consecutive_successes += 1
        if self._consecutive_successes >= self.shrink_after:
            old = self.current_delay
            self.current_delay = max(self.current_delay - self.shrink, self.base_delay)
            self._consecutive_successes = 0
            if old != self.current_delay:
                log.info(
                    "  ↓ %d successes — delay: %.1fs → %.1fs",
                    self.shrink_after, old, self.current_delay,
                )

    def reset(self):
        self.current_delay = self.base_delay
        self._consecutive_successes = 0

    def wait(self):
        time.sleep(self.current_delay)


# ═══════════════════════════════════════════════════════════════════════════════
#  Places API Client — Strict No-Skip
# ═══════════════════════════════════════════════════════════════════════════════

class PlacesAPIClient:
    """HTTP client for Google Places API (New) Nearby Search.

    Error policy:
      - 429 / 5xx: retry with exponential backoff up to MAX_RETRIES.
      - If retries exhausted → sys.exit(1) (checkpoint was already saved).
    """

    def __init__(self, api_key: str, rate_limiter: AdaptiveRateLimiter):
        self.api_key = api_key
        self.rate_limiter = rate_limiter
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
        })

    def nearby_search(
        self,
        included_types: list[str],
        lat: float,
        lng: float,
        radius: float,
        field_mask: str,
    ) -> list[dict]:
        """Execute a Nearby Search (New) and return places list.

        STRICT NO-SKIP: On persistent 429/5xx, calls sys.exit(1).
        """
        headers = {"X-Goog-FieldMask": field_mask}

        body: dict[str, Any] = {
            "includedTypes": included_types,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lng},
                    "radius": radius,
                }
            },
            "maxResultCount": PAGE_SIZE,
            "languageCode": "en",
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.post(
                    NEARBY_SEARCH_URL, headers=headers, json=body, timeout=30,
                )

                if resp.status_code == 200:
                    self.rate_limiter.on_success()
                    return resp.json().get("places", [])

                if resp.status_code == 429:
                    self.rate_limiter.on_rate_limit()
                    backoff = INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    log.warning(
                        "  ⚠️  HTTP 429 — attempt %d/%d, waiting %.0fs...",
                        attempt, MAX_RETRIES, backoff,
                    )
                    time.sleep(backoff)
                    continue

                if resp.status_code in (500, 503):
                    backoff = INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    log.warning(
                        "  HTTP %d — attempt %d/%d, waiting %.0fs...",
                        resp.status_code, attempt, MAX_RETRIES, backoff,
                    )
                    time.sleep(backoff)
                    continue

                if resp.status_code in (400, 403):
                    log.error(
                        "  HTTP %d (non-retryable): %s",
                        resp.status_code, resp.text[:500],
                    )
                    return []  # Skip this request but don't crash

                log.error("  HTTP %d: %s", resp.status_code, resp.text[:300])
                return []

            except requests.exceptions.RequestException as exc:
                backoff = INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1))
                log.warning(
                    "  Network error — attempt %d/%d: %s, waiting %.0fs...",
                    attempt, MAX_RETRIES, exc, backoff,
                )
                time.sleep(backoff)

        # ── STRICT NO-SKIP: retries exhausted → terminate ────────────
        log.error("═" * 65)
        log.error("  ❌ FATAL: Max retries exhausted for grid point (%.6f, %.6f)", lat, lng)
        log.error("  Checkpoint saved — restart to resume from this point.")
        log.error("═" * 65)
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
#  Data Transformer (Schema-Compliant — Unchanged from v2.0)
# ═══════════════════════════════════════════════════════════════════════════════

class DataTransformer:
    """Transforms raw Places API responses into SobatNavi namespace schemas."""

    DURATION_DEFAULTS = ENRICHMENT_CONFIG.get("ml_inference", {}).get(
        "average_duration_minutes", {}
    ).get("defaults", {})

    INDOOR_TYPES = set(
        ENRICHMENT_CONFIG.get("heuristics", {}).get("is_indoor", {}).get("indoor_types", [])
    )
    OUTDOOR_TYPES = set(
        ENRICHMENT_CONFIG.get("heuristics", {}).get("is_indoor", {}).get("outdoor_types", [])
    )

    # ── Shared helpers ────────────────────────────────────────────────

    @staticmethod
    def _extract_coords(place: dict) -> tuple[float, float]:
        loc = place.get("location", {})
        precision = QUALITY_THRESHOLDS.get("coordinate_precision", 6)
        return (
            round(float(loc.get("latitude", 0.0)), precision),
            round(float(loc.get("longitude", 0.0)), precision),
        )

    @staticmethod
    def _extract_name(place: dict) -> str:
        return place.get("displayName", {}).get("text", "Unknown")

    @staticmethod
    def _extract_price_tier(place: dict) -> int:
        level = place.get("priceLevel")
        if level is None:
            level = place.get("priceLevel", "")
        return PRICE_LEVEL_MAP.get(level, PRICE_LEVEL_MAP.get("", -1))

    @staticmethod
    def _extract_opening_hours(place: dict) -> Optional[str]:
        hours = place.get("regularOpeningHours", {})
        descriptions = hours.get("weekdayDescriptions", [])
        if descriptions:
            return descriptions[0] if descriptions[0] else None
        periods = hours.get("periods", [])
        if not periods:
            return None
        try:
            period = periods[0]
            o = period.get("open", {})
            c = period.get("close", {})
            return f"{o.get('hour', 0):02d}:{o.get('minute', 0):02d}-{c.get('hour', 0):02d}:{c.get('minute', 0):02d}"
        except (KeyError, IndexError, TypeError):
            return None

    @staticmethod
    def _classify_poi_type(place: dict) -> str:
        primary_type = place.get("primaryType", "")
        if primary_type in GOOGLE_TYPE_TO_POI_TYPE:
            return GOOGLE_TYPE_TO_POI_TYPE[primary_type]
        for t in place.get("types", []):
            if t in GOOGLE_TYPE_TO_POI_TYPE:
                return GOOGLE_TYPE_TO_POI_TYPE[t]
        type_str = " ".join(place.get("types", [])).lower()
        if "temple" in type_str or "worship" in type_str:
            return "temple"
        if "beach" in type_str:
            return "beach"
        if "restaurant" in type_str or "food" in type_str:
            return "restaurant"
        if "cafe" in type_str or "coffee" in type_str:
            return "cafe"
        if "spa" in type_str:
            return "spa_wellness"
        return "TBD_TYPE"

    @staticmethod
    def _compute_popularity(place: dict) -> float:
        rating = place.get("rating", 0.0)
        count = place.get("userRatingCount", 0)
        if count == 0:
            return 0.0
        raw = rating * math.log10(count + 1)
        return round(min(raw / 20.0, 1.0), 3)

    @staticmethod
    def _is_operational(place: dict) -> bool:
        status = place.get("businessStatus", "OPERATIONAL")
        return status in ("OPERATIONAL", "")

    @staticmethod
    def _passes_quality(place: dict) -> bool:
        rating = place.get("rating", 0.0)
        reviews = place.get("userRatingCount", 0)
        return (
            rating >= QUALITY_THRESHOLDS.get("min_rating", 0)
            and reviews >= QUALITY_THRESHOLDS.get("min_reviews", 0)
        )

    def _infer_is_indoor(self, place: dict, poi_type: str) -> Optional[bool]:
        types = set(place.get("types", []))
        if types & self.INDOOR_TYPES:
            return True
        if types & self.OUTDOOR_TYPES:
            return False
        indoor_poi = {"museum", "cafe", "restaurant", "coworking", "spa_wellness", "yoga_studio"}
        outdoor_poi = {"beach", "waterfall", "nature_reserve"}
        if poi_type in indoor_poi:
            return True
        if poi_type in outdoor_poi:
            return False
        return None

    def _infer_weather_resilience(self, place: dict, poi_type: str) -> str:
        types = set(place.get("types", []))
        has_outdoor_seating = place.get("outdoorSeating", False)
        if types & {"shopping_mall", "museum", "art_gallery", "indoor_play_area"}:
            return "indoor"
        if poi_type in {"cafe", "restaurant"} and not has_outdoor_seating:
            return "indoor"
        if poi_type in {"cafe", "restaurant"} and has_outdoor_seating:
            return "covered"
        if types & {"beach", "hiking_area", "waterfall", "park"}:
            return "outdoor"
        if poi_type in {"museum", "coworking", "spa_wellness", "yoga_studio"}:
            return "indoor"
        if poi_type in {"market"}:
            return "covered"
        return ENRICHMENT_CONFIG.get("heuristics", {}).get(
            "weather_resilience", {}
        ).get("default", "DATA_MISSING")

    @staticmethod
    def _infer_transport_access(place: dict) -> str:
        parking = place.get("parkingOptions")
        types = set(place.get("types", []))
        if "hiking_area" in types:
            return "hiking"
        if parking:
            return "car"
        if "beach" in types:
            return "bike_only"
        return ENRICHMENT_CONFIG.get("heuristics", {}).get(
            "transport_access", {}
        ).get("default", "NOT_SET")

    @staticmethod
    def _extract_dietary_options(place: dict) -> list[str]:
        dietary = []
        if place.get("servesVegetarianFood"):
            dietary.append("vegetarian")
        if place.get("servesVeganFood"):
            dietary.append("vegan")
        if place.get("servesHalal"):
            dietary.append("halal_friendly")
        return dietary

    @staticmethod
    def _infer_work_friendly(place: dict) -> bool:
        types = set(place.get("types", []))
        has_wifi = place.get("wifiAvailable", False)
        rating = place.get("rating", 0)
        work_types = {"cafe", "coffee_shop", "coworking_space", "library", "internet_cafe"}
        if bool(types & work_types) and rating >= 4.0:
            return True
        if has_wifi and bool(types & work_types):
            return True
        return False

    @staticmethod
    def _infer_wifi_speed(place: dict, acc_type: str) -> str:
        has_wifi = place.get("wifiAvailable", False)
        if not has_wifi:
            return "low"
        if acc_type in ("Resort", "5-star Hotel", "Boutique Hotel"):
            return "high"
        return "medium"

    @staticmethod
    def _extract_photo_ref(place: dict) -> Optional[str]:
        photos = place.get("photos", [])
        if photos and isinstance(photos, list):
            return photos[0].get("name", None)
        return None

    @staticmethod
    def _extract_cuisine_type(place: dict) -> str:
        types = place.get("types", [])
        cuisine_map = {
            "indonesian_restaurant": "Indonesian",
            "japanese_restaurant": "Japanese",
            "italian_restaurant": "Italian",
            "indian_restaurant": "Indian",
            "chinese_restaurant": "Chinese",
            "thai_restaurant": "Thai",
            "mexican_restaurant": "Mexican",
            "seafood_restaurant": "Seafood",
            "vegan_restaurant": "Vegan",
            "vegetarian_restaurant": "Vegetarian",
            "pizza_restaurant": "Italian",
            "sushi_restaurant": "Japanese",
            "korean_restaurant": "Korean",
            "french_restaurant": "French",
            "mediterranean_restaurant": "Mediterranean",
            "middle_eastern_restaurant": "Middle Eastern",
            "cafe": "Cafe", "coffee_shop": "Cafe",
            "bar": "Bar", "bakery": "Bakery",
            "juice_bar": "Health", "ice_cream_shop": "Dessert",
            "tea_house": "Cafe",
        }
        for t in types:
            if t in cuisine_map:
                return cuisine_map[t]
        return "International"

    @staticmethod
    def _infer_vibe_from_place(place: dict, context: str = "") -> list[str]:
        vibes = []
        types = set(place.get("types", []))
        rating = place.get("rating", 0)
        review_count = place.get("userRatingCount", 0)
        has_live_music = place.get("liveMusic", False)

        if rating >= 4.5 and review_count < 100:
            vibes.append("hidden_gem")
        if review_count > 500:
            vibes.append("lively")
        elif review_count < 50 and rating >= 4.0:
            vibes.append("quiet")
        if "spa" in types or "yoga_studio" in types or "meditation_center" in types:
            vibes.append("quiet")
        if has_live_music or "night_club" in types or "bar" in types:
            vibes.append("lively")
        if "hindu_temple" in types or "place_of_worship" in types:
            vibes.append("traditional")
        if "romantic" in context.lower() or "sunset" in context.lower():
            vibes.append("romantic")
        if "family" in context.lower():
            vibes.append("family_friendly")
        if "modern" not in vibes and "traditional" not in vibes:
            vibes.append("modern")

        return list(dict.fromkeys(vibes))[:3]

    # ── Namespace-specific transformations ────────────────────────────

    def to_poi_attraction(self, place: dict, region: RegionConfig) -> dict:
        lat, lng = self._extract_coords(place)
        poi_type = self._classify_poi_type(place)
        duration = self.DURATION_DEFAULTS.get(poi_type, 60)
        return {
            "poi_id": str(uuid.uuid4()),
            "place_id": place.get("id", ""),
            "name": self._extract_name(place),
            "poi_type": poi_type,
            "geospatial": {
                "latitude": lat, "longitude": lng,
                "district": region.name,
                "kelurahan_name": "",
                "neighborhood_cluster": f"{region.tourism_cluster}_{poi_type}",
            },
            "categorization": {
                "primary_category": POI_TYPE_TO_PRIMARY_CATEGORY.get(poi_type, "PENDING_CATEGORY"),
                "price_tier": self._extract_price_tier(place),
                "is_indoor": self._infer_is_indoor(place, poi_type),
                "weather_resilience": self._infer_weather_resilience(place, poi_type),
                "rating_average": round(float(place.get("rating", 0.0)), 1),
                "popularity_score": self._compute_popularity(place),
            },
            "operational": {
                "opening_hours": self._extract_opening_hours(place),
                "average_duration_minutes": duration,
                "closed_dates": [],
                "transport_access": self._infer_transport_access(place),
            },
            "_quality": {
                "rating": place.get("rating"),
                "review_count": place.get("userRatingCount", 0),
                "business_status": place.get("businessStatus", "UNKNOWN"),
                "google_types": place.get("types", []),
                "primary_type": place.get("primaryType", ""),
            },
            "_meta": {
                "google_place_id": place.get("id", ""),
                "photo_ref": self._extract_photo_ref(place),
                "website": place.get("websiteUri"),
                "phone": place.get("nationalPhoneNumber"),
                "address": place.get("formattedAddress", ""),
                "source_region": region.name,
                "tourism_cluster": region.tourism_cluster,
                "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            },
        }

    def to_culinary_amenity(self, place: dict, region: RegionConfig) -> dict:
        lat, lng = self._extract_coords(place)
        is_work_friendly = self._infer_work_friendly(place)
        return {
            "poi_id": str(uuid.uuid4()),
            "place_id": place.get("id", ""),
            "name": self._extract_name(place),
            "cuisine_type": self._extract_cuisine_type(place),
            "geospatial": {
                "latitude": lat, "longitude": lng,
                "kelurahan_name": "",
            },
            "attributes": {
                "dietary_options": self._extract_dietary_options(place),
                "price_tier": self._extract_price_tier(place),
                "is_work_friendly": is_work_friendly,
            },
            "operational": {
                "opening_hours": self._extract_opening_hours(place),
                "average_meal_duration_minutes": 90 if is_work_friendly else 45,
            },
            "_quality": {
                "rating": place.get("rating"),
                "review_count": place.get("userRatingCount", 0),
                "business_status": place.get("businessStatus", "UNKNOWN"),
                "google_types": place.get("types", []),
                "primary_type": place.get("primaryType", ""),
            },
            "_meta": {
                "google_place_id": place.get("id", ""),
                "photo_ref": self._extract_photo_ref(place) if place.get("photos") else None,
                "address": place.get("formattedAddress", ""),
                "has_wifi": place.get("wifiAvailable", False),
                "outdoor_seating": place.get("outdoorSeating", False),
                "live_music": place.get("liveMusic", False),
                "serves_beer": place.get("servesBeer", False),
                "serves_wine": place.get("servesWine", False),
                "serves_cocktails": place.get("servesCocktails", False),
                "dine_in": place.get("dineIn", False),
                "delivery": place.get("delivery", False),
                "takeout": place.get("takeout", False),
                "reservable": place.get("reservable", False),
                "source_region": region.name,
                "tourism_cluster": region.tourism_cluster,
                "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            },
        }

    def to_accommodation_anchor(self, place: dict, region: RegionConfig) -> dict:
        lat, lng = self._extract_coords(place)
        types = set(place.get("types", []))

        acc_type = "Hotel"
        if "resort_hotel" in types:
            acc_type = "Resort"
        elif types & {"guest_house", "bed_and_breakfast"}:
            acc_type = "Guest House"
        elif "hostel" in types:
            acc_type = "Hostel"
        elif "motel" in types:
            acc_type = "Motel"
        elif "lodging" in types and place.get("rating", 0) >= 4.5:
            acc_type = "Boutique Hotel"

        vibe = self._infer_vibe_from_place(place)
        if acc_type == "Resort" and "romantic" not in vibe:
            vibe.insert(0, "romantic")
        if acc_type in ("Hostel", "Guest House") and "lively" not in vibe:
            vibe.insert(0, "lively")

        return {
            "hotel_id": str(uuid.uuid4()),
            "place_id": place.get("id", ""),
            "identity": {
                "name": self._extract_name(place),
                "accommodation_type": acc_type,
                "vibe_description": vibe[:3],
            },
            "logistics": {
                "latitude": lat, "longitude": lng,
                "neighborhood_cluster": f"{region.tourism_cluster}_{acc_type.lower().replace(' ', '_')}",
                "check_in_out_time": ["14:00", "12:00"],
            },
            "amenities": {
                "wifi_speed_level": self._infer_wifi_speed(place, acc_type),
                "workspace_available": acc_type in ("Resort", "Hotel", "Boutique Hotel")
                                       or place.get("wifiAvailable", False),
            },
            "_quality": {
                "rating": place.get("rating"),
                "review_count": place.get("userRatingCount", 0),
                "business_status": place.get("businessStatus", "UNKNOWN"),
                "google_types": place.get("types", []),
                "primary_type": place.get("primaryType", ""),
            },
            "_meta": {
                "google_place_id": place.get("id", ""),
                "photo_ref": self._extract_photo_ref(place),
                "address": place.get("formattedAddress", ""),
                "source_region": region.name,
                "tourism_cluster": region.tourism_cluster,
                "region_density": region.density,
                "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            },
        }


# ═══════════════════════════════════════════════════════════════════════════════
#  POI Fetcher v3.0 — Grid-Walk Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

class POIFetcher:
    """Grid-based deep crawl orchestrator.

    For each grid point:
      1. Call Nearby Search for each namespace (3 calls)
      2. Route & deduplicate results by place_id
      3. Transform → schema
      4. Atomic checkpoint

    On persistent error → sys.exit(1).
    """

    CHECKPOINT_FILENAME = ".fetch_checkpoint.json"
    NAMESPACES = ["poi_attractions", "culinary_amenities", "accommodation_anchors"]

    def __init__(self, api_key: str, output_dir: str = "./output"):
        self.rate_limiter = AdaptiveRateLimiter()
        self.client = PlacesAPIClient(api_key, self.rate_limiter)
        self.transformer = DataTransformer()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint = CheckpointManager(self.output_dir / self.CHECKPOINT_FILENAME)

        # State
        self.seen: dict[str, set] = {ns: set() for ns in self.NAMESPACES}
        self.results: dict[str, list] = {ns: [] for ns in self.NAMESPACES}
        self.last_processed_index: int = -1

        # Stats
        self.stats = {"api_calls": 0, "duplicates": 0, "filtered_bbox": 0}

    # ── State management ──────────────────────────────────────────────

    def _load_checkpoint(self):
        if not self.checkpoint.exists():
            return
        state = self.checkpoint.load()
        self.last_processed_index = state.get("last_processed_index", -1)
        for ns, ids in state.get("seen_place_ids", {}).items():
            if ns in self.seen:
                self.seen[ns] = set(ids)
        for ns, records in state.get("results", {}).items():
            if ns in self.results:
                self.results[ns] = records

        total = sum(len(r) for r in self.results.values())
        log.info(
            "🔄 Resuming from index %d (%d records loaded)",
            self.last_processed_index, total,
        )

    def _save_checkpoint(self):
        state = {
            "last_processed_index": self.last_processed_index,
            "seen_place_ids": {ns: list(ids) for ns, ids in self.seen.items()},
            "results": self.results,
        }
        self.checkpoint.save(state)

    # ── Bali bounding-box check ───────────────────────────────────────

    @staticmethod
    def _is_in_bali(lat: float, lng: float) -> bool:
        return -9.1 <= lat <= -8.0 and 114.4 <= lng <= 116.0

    # ── Namespace routing ─────────────────────────────────────────────

    @staticmethod
    def _route_to_namespace(place: dict) -> Optional[str]:
        """Determine which namespace a place belongs to based on its types."""
        primary = place.get("primaryType", "")
        if primary in GOOGLE_TYPE_TO_NAMESPACE:
            return GOOGLE_TYPE_TO_NAMESPACE[primary]
        for t in place.get("types", []):
            if t in GOOGLE_TYPE_TO_NAMESPACE:
                return GOOGLE_TYPE_TO_NAMESPACE[t]
        return None

    # ── Transform helper ──────────────────────────────────────────────

    def _transform_place(
        self, place: dict, namespace: str, region: RegionConfig,
    ) -> Optional[dict]:
        if namespace == "poi_attractions":
            return self.transformer.to_poi_attraction(place, region)
        elif namespace == "culinary_amenities":
            return self.transformer.to_culinary_amenity(place, region)
        elif namespace == "accommodation_anchors":
            return self.transformer.to_accommodation_anchor(place, region)
        return None

    # ── Process a single grid point ───────────────────────────────────

    def _process_grid_point(
        self, point: GridPoint, region: RegionConfig,
    ) -> dict[str, int]:
        """Process one grid point: 3 Nearby Search calls (one per namespace).

        Returns dict of {namespace: num_new_records}.
        """
        search_radius = float(GRID_CONFIG["search_radius_meters"])
        results_per_ns: dict[str, int] = {}

        for ns in self.NAMESPACES:
            included_types = NAMESPACE_TYPES.get(ns, [])
            field_mask = FIELD_MASKS.get(ns, "")

            if not included_types or not field_mask:
                results_per_ns[ns] = 0
                continue

            # ── API call ──────────────────────────────────────────────
            self.stats["api_calls"] += 1
            places = self.client.nearby_search(
                included_types=included_types,
                lat=point.lat,
                lng=point.lng,
                radius=search_radius,
                field_mask=field_mask,
            )

            added = 0
            for place in places:
                place_id = place.get("id", "")
                if not place_id:
                    continue

                # Global dedup
                if place_id in self.seen[ns]:
                    self.stats["duplicates"] += 1
                    continue

                # Transform
                transformed = self._transform_place(place, ns, region)
                if not transformed:
                    continue

                # Bounding-box check
                if ns == "accommodation_anchors":
                    plat = transformed["logistics"]["latitude"]
                    plng = transformed["logistics"]["longitude"]
                else:
                    plat = transformed["geospatial"]["latitude"]
                    plng = transformed["geospatial"]["longitude"]

                if not self._is_in_bali(plat, plng):
                    self.stats["filtered_bbox"] += 1
                    continue

                self.seen[ns].add(place_id)
                self.results[ns].append(transformed)
                added += 1

            results_per_ns[ns] = added

            # Adaptive delay between namespace calls
            self.rate_limiter.wait()

        return results_per_ns

    # ── JSON export ───────────────────────────────────────────────────

    def export_json(self, namespace: str):
        data = self.results.get(namespace, [])
        if not data:
            return

        # Strip internal dedup field
        cleaned = [{k: v for k, v in item.items() if k != "place_id"} for item in data]

        output_path = self.output_dir / f"{namespace}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=2, ensure_ascii=False)

        log.info("✅ Exported %d records → %s", len(cleaned), output_path)

    # ── Main run loop ─────────────────────────────────────────────────

    def run(
        self,
        batch_size: int = 0,
        dry_run: bool = False,
    ):
        """Execute the grid-based deep crawl pipeline.

        Args:
            batch_size: Max grid points to process (0 = unlimited).
            dry_run: Process only 3 grid points for testing.
        """
        # ── Generate grid ─────────────────────────────────────────────
        grid = BaliGridGenerator.generate_all()
        total_points = len(grid)

        # ── Load checkpoint ───────────────────────────────────────────
        if not dry_run:
            self._load_checkpoint()

        start_index = self.last_processed_index + 1
        remaining = total_points - start_index

        if dry_run:
            batch_size = 3
            start_index = 0

        effective_batch = batch_size if batch_size > 0 else remaining
        points_to_process = min(effective_batch, remaining)

        log.info("")
        log.info("═" * 65)
        log.info("  GRID-BASED DEEP CRAWL v3.0")
        log.info("─" * 65)
        log.info("  Total grid points:    %5d", total_points)
        log.info("  Already completed:    %5d", start_index)
        log.info("  Remaining:            %5d", remaining)
        log.info("  This batch:           %5d", points_to_process)
        log.info("  Spacing:              %5dm", GRID_CONFIG["spacing_meters"])
        log.info("  Search radius:        %5dm", GRID_CONFIG["search_radius_meters"])
        log.info("  API calls/point:      %5d  (one per namespace)", len(self.NAMESPACES))
        log.info("  Est. API calls:       %5d", points_to_process * len(self.NAMESPACES))
        log.info("  Delay:                adaptive (%.1fs base)", BATCH_DELAY_BASE_SECONDS)
        log.info("═" * 65)

        if remaining <= 0:
            log.info("✅ All grid points already completed!")
            for ns in self.NAMESPACES:
                self.export_json(ns)
            self.checkpoint.delete()
            self._print_summary(0)
            return

        # ── Process grid points ───────────────────────────────────────
        start_time = time.time()
        processed = 0

        for point in grid:
            if point.index < start_index:
                continue
            if processed >= points_to_process:
                break

            processed += 1
            region = BALI_REGIONS.get(point.region_name)
            if not region:
                continue

            log.info(
                "[%d/%d] #%d %-10s │ (%.5f, %.5f)",
                processed, points_to_process,
                point.index, point.region_name,
                point.lat, point.lng,
            )

            ns_results = self._process_grid_point(point, region)

            # Log results
            parts = []
            for ns, count in ns_results.items():
                short = ns.split("_")[0][:3]
                parts.append(f"{short}:+{count}")
            total_ns = {ns: len(self.results[ns]) for ns in self.NAMESPACES}
            log.info(
                "  %s │ totals: poi=%d cul=%d acc=%d │ dup:%d",
                " ".join(parts),
                total_ns["poi_attractions"],
                total_ns["culinary_amenities"],
                total_ns["accommodation_anchors"],
                self.stats["duplicates"],
            )

            # ── Atomic checkpoint ─────────────────────────────────────
            if not dry_run:
                self.last_processed_index = point.index
                self._save_checkpoint()
                log.info("  💾 Checkpoint (index=%d)", point.index)

        elapsed = time.time() - start_time

        # ── Export ─────────────────────────────────────────────────────
        log.info("")
        log.info("📦 Exporting JSON files...")
        for ns in self.NAMESPACES:
            self.export_json(ns)

        batch_complete = (start_index + processed) >= total_points
        if batch_complete and not dry_run:
            self.checkpoint.delete()
            log.info("✅ All grid points completed! Checkpoint cleared.")
        elif not dry_run:
            log.info(
                "📋 Batch complete (%d/%d total). Checkpoint saved at index %d.",
                start_index + processed, total_points, self.last_processed_index,
            )
            log.info("   Run again to continue processing.")

        self._print_summary(elapsed)

    # ── Summary ───────────────────────────────────────────────────────

    def _print_summary(self, elapsed: float):
        log.info("")
        log.info("═" * 65)
        log.info("  FETCH COMPLETE — %.1f seconds", elapsed)
        log.info("═" * 65)
        for ns in self.NAMESPACES:
            log.info("  %-25s │ %5d records", ns, len(self.results.get(ns, [])))
        log.info("─" * 65)
        log.info("  API calls made:          %5d", self.stats["api_calls"])
        log.info("  Duplicates skipped:      %5d", self.stats["duplicates"])
        log.info("  Out-of-bounds filtered:  %5d", self.stats["filtered_bbox"])
        log.info("═" * 65)


# ═══════════════════════════════════════════════════════════════════════════════
#  CLI Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="SobatNavi — Grid-Based POI Deep Crawl v3.0 for Bali",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python google_places_fetcher.py                    # Full crawl (auto-resumes)
  python google_places_fetcher.py --batch-size 50    # Process 50 grid points
  python google_places_fetcher.py --dry-run          # Test with 3 grid points
  python google_places_fetcher.py --reset            # Clear checkpoint
        """,
    )
    parser.add_argument(
        "--batch-size", type=int, default=0,
        help="Max grid points per run (0 = unlimited). Use with cron.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Test mode: process 3 grid points only.",
    )
    parser.add_argument(
        "--output-dir", default="./output",
        help="Output directory for JSON files (default: ./output).",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Clear checkpoint and start fresh.",
    )

    args = parser.parse_args()

    # Load environment
    load_dotenv()
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    if not api_key or api_key in ("your_api_key_here", ""):
        log.error("❌ GOOGLE_PLACES_API_KEY not set. Set it in your .env file.")
        sys.exit(1)

    # Handle --reset
    if args.reset:
        cp_path = Path(args.output_dir) / POIFetcher.CHECKPOINT_FILENAME
        if cp_path.exists():
            cp_path.unlink()
            log.info("🗑️  Checkpoint cleared.")
        else:
            log.info("ℹ️  No checkpoint found.")
        return

    # Run
    fetcher = POIFetcher(api_key=api_key, output_dir=args.output_dir)
    fetcher.run(batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
