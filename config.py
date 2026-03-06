"""
Configuration for SobatNavi Google Places API (New) POI Fetcher v3.0
Grid-Based Deep Crawl — Research-Grade Configuration
Generated: 2026-03-06
"""

import os
from typing import Dict, List, Optional
from dataclasses import dataclass

# ─────────────────────────────────────────────────────────────────────────────
# API CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

GOOGLE_PLACES_API_KEY: str = os.getenv("GOOGLE_PLACES_API_KEY", "")
NEARBY_SEARCH_URL: str = "https://places.googleapis.com/v1/places:searchNearby"

# ─────────────────────────────────────────────────────────────────────────────
# GRID CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
# Strategy: tile each tourism zone with overlapping circles on a hex-ish grid.
# At 1km spacing with 500m search radius, adjacent circles overlap by ~100m,
# ensuring no POI falls through the cracks.

GRID_CONFIG = {
    "spacing_meters": 1000,        # Distance between grid points
    "search_radius_meters": 500,   # Nearby Search circle radius per grid point
}

# ─────────────────────────────────────────────────────────────────────────────
# RATE LIMITING — Adaptive + Strict Fail-Safe
# ─────────────────────────────────────────────────────────────────────────────

BATCH_DELAY_BASE_SECONDS: float = 2.0      # Starting delay (optimistic)
BATCH_DELAY_MAX_SECONDS: float = 30.0      # Never wait longer than this between calls
BATCH_DELAY_INCREMENT: float = 3.0         # Add this on every 429 hit
BATCH_DELAY_SHRINK: float = 0.5            # Subtract this after consecutive successes
BATCH_DELAY_SHRINK_AFTER: int = 5          # Shrink after N consecutive successes
MAX_RETRIES: int = 5                       # Retries per request (429 / 5xx / network)
INITIAL_BACKOFF_SECONDS: float = 2.0       # First retry wait (doubles each attempt)
PAGE_SIZE: int = 20                        # Google Places API (New) hard limit

# ─────────────────────────────────────────────────────────────────────────────
# BALI REGION DEFINITIONS (Tourism Zones for Grid Generation)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RegionConfig:
    name: str
    lat: float
    lng: float
    radius: int  # meters — defines the zone boundary for grid tiling
    density: str  # high|medium|low
    tourism_cluster: str
    primary_personas: List[str]

BALI_REGIONS: Dict[str, RegionConfig] = {
    # SOUTH BALI — High Density Tourism Corridor
    "Kuta": RegionConfig(
        name="Kuta", lat=-8.7220, lng=115.1725, radius=5000,
        density="high", tourism_cluster="south_beach_corridor",
        primary_personas=["Backpacker", "Family", "Adventure Junkie"],
    ),
    "Legian": RegionConfig(
        name="Legian", lat=-8.7036, lng=115.1686, radius=3500,
        density="high", tourism_cluster="south_beach_corridor",
        primary_personas=["Family", "Backpacker"],
    ),
    "Seminyak": RegionConfig(
        name="Seminyak", lat=-8.6897, lng=115.1568, radius=4000,
        density="high", tourism_cluster="south_beach_corridor",
        primary_personas=["Luxury Traveler", "Digital Nomad", "Food_Beverage"],
    ),
    "Canggu": RegionConfig(
        name="Canggu", lat=-8.6478, lng=115.1385, radius=5000,
        density="high", tourism_cluster="south_beach_corridor",
        primary_personas=["Digital Nomad", "Backpacker", "Surf"],
    ),
    # SOUTH COAST — Resort & Luxury Clusters
    "Jimbaran": RegionConfig(
        name="Jimbaran", lat=-8.7916, lng=115.1606, radius=5000,
        density="medium", tourism_cluster="south_luxury_bay",
        primary_personas=["Luxury Traveler", "Family", "Food_Beverage"],
    ),
    "Uluwatu": RegionConfig(
        name="Uluwatu", lat=-8.8291, lng=115.0849, radius=6000,
        density="medium", tourism_cluster="bukit_peninsula",
        primary_personas=["Luxury Traveler", "Spiritual Seeker", "Adventure Junkie"],
    ),
    "Nusa_Dua": RegionConfig(
        name="Nusa_Dua", lat=-8.8005, lng=115.2326, radius=5000,
        density="medium", tourism_cluster="south_luxury_bay",
        primary_personas=["Luxury Traveler", "Family", "MICE"],
    ),
    "Sanur": RegionConfig(
        name="Sanur", lat=-8.6860, lng=115.2630, radius=4000,
        density="medium", tourism_cluster="east_coast_family",
        primary_personas=["Family", "Retiree", "Spiritual Seeker"],
    ),
    # CENTRAL BALI — Cultural & Wellness Hub
    "Ubud": RegionConfig(
        name="Ubud", lat=-8.5069, lng=115.2625, radius=8000,
        density="medium", tourism_cluster="central_cultural_heart",
        primary_personas=["Spiritual Seeker", "Digital Nomad", "Wellness", "Culture"],
    ),
    "Sidemen": RegionConfig(
        name="Sidemen", lat=-8.4833, lng=115.4333, radius=7000,
        density="low", tourism_cluster="east_cultural_corridor",
        primary_personas=["Spiritual Seeker", "Adventure Junkie", "Culture"],
    ),
    # MOUNTAIN & LAKE REGIONS
    "Kintamani": RegionConfig(
        name="Kintamani", lat=-8.2413, lng=115.3700, radius=10000,
        density="low", tourism_cluster="mountain_volcano_zone",
        primary_personas=["Adventure Junkie", "Nature", "Photography"],
    ),
    "Bedugul": RegionConfig(
        name="Bedugul", lat=-8.2850, lng=115.1680, radius=6000,
        density="low", tourism_cluster="mountain_lake_zone",
        primary_personas=["Nature", "Family", "Spiritual Seeker"],
    ),
    "Munduk": RegionConfig(
        name="Munduk", lat=-8.2675, lng=115.0950, radius=8000,
        density="low", tourism_cluster="north_waterfall_zone",
        primary_personas=["Adventure Junkie", "Nature", "Photography"],
    ),
    # EAST & NORTH BALI — Marine & Cultural
    "Amed": RegionConfig(
        name="Amed", lat=-8.3492, lng=115.6510, radius=8000,
        density="low", tourism_cluster="east_marine_corridor",
        primary_personas=["Adventure Junkie", "Diving", "Backpacker"],
    ),
    "Lovina": RegionConfig(
        name="Lovina", lat=-8.1500, lng=115.0333, radius=10000,
        density="low", tourism_cluster="north_beach_zone",
        primary_personas=["Family", "Backpacker", "Nature"],
    ),
}

# ─────────────────────────────────────────────────────────────────────────────
# NAMESPACE TYPES — Flat type lists per namespace for Nearby Search
# ─────────────────────────────────────────────────────────────────────────────
# Each grid point gets ONE Nearby Search call per namespace.
# Max 50 includedTypes per call (Google API limit).

NAMESPACE_TYPES: Dict[str, List[str]] = {
    "poi_attractions": [
        # Religious/Cultural
        "hindu_temple", "buddhist_temple", "church", "mosque", "synagogue",
        # Nature
        "national_park", "park", "hiking_area", "state_park",
        "scenic_spot", "wildlife_park", "zoo",
        # Beaches
        "beach",
        # Museums/Cultural
        "museum", "art_gallery", "art_museum", "history_museum",
        "historical_landmark", "cultural_landmark",
        # Wellness
        "spa", "wellness_center", "yoga_studio",
        # Coworking
        "coworking_space", "library",
        # Attractions
        "tourist_attraction", "visitor_center", "botanical_garden",
        "aquarium", "amusement_park",
    ],  # 29 types — under 50 limit

    "culinary_amenities": [
        # Restaurants
        "restaurant", "food_court", "meal_takeaway",
        "indonesian_restaurant", "seafood_restaurant",
        # Cafes
        "cafe", "coffee_shop", "bakery", "juice_shop",
        "tea_house", "ice_cream_shop",
        # Bars & Nightlife
        "bar", "night_club", "pub", "wine_bar", "cocktail_bar",
    ],  # 16 types — under 50 limit

    "accommodation_anchors": [
        # Hotels & Resorts
        "hotel", "resort_hotel", "extended_stay_hotel",
        # Budget & Alternative
        "hostel", "guest_house", "bed_and_breakfast",
        "motel", "lodging", "inn", "cottage",
    ],  # 10 types — under 50 limit
}

# ─────────────────────────────────────────────────────────────────────────────
# FIELD MASKS (Cost-Optimized per Namespace)
# ─────────────────────────────────────────────────────────────────────────────

FIELD_MASKS: Dict[str, str] = {
    "poi_attractions": ",".join([
        "places.id", "places.displayName", "places.formattedAddress",
        "places.location", "places.types", "places.primaryType",
        "places.rating", "places.userRatingCount",
        "places.regularOpeningHours", "places.priceLevel",
        "places.photos", "places.parkingOptions", "places.accessibilityOptions",
        "places.businessStatus", "places.nationalPhoneNumber", "places.websiteUri",
    ]),

    "culinary_amenities": ",".join([
        "places.id", "places.displayName", "places.formattedAddress",
        "places.location", "places.types", "places.primaryType",
        "places.rating", "places.userRatingCount",
        "places.regularOpeningHours", "places.priceLevel",
        "places.servesVegetarianFood",
        "places.dineIn", "places.delivery", "places.takeout", "places.reservable",
        "places.outdoorSeating", "places.liveMusic",
        "places.servesBeer", "places.servesWine", "places.servesCocktails",
        "places.goodForGroups", "places.goodForChildren",
        "places.businessStatus",
    ]),

    "accommodation_anchors": ",".join([
        "places.id", "places.displayName", "places.formattedAddress",
        "places.location", "places.types", "places.primaryType",
        "places.rating", "places.userRatingCount",
        "places.priceLevel", "places.regularOpeningHours",
        "places.businessStatus", "places.photos",
    ]),
}

# ─────────────────────────────────────────────────────────────────────────────
# TYPE MAPPINGS (Google Places → SobatNavi Schema)
# ─────────────────────────────────────────────────────────────────────────────

GOOGLE_TYPE_TO_POI_TYPE: Dict[str, str] = {
    # Religious/Cultural
    "hindu_temple": "temple", "buddhist_temple": "temple",
    "place_of_worship": "temple", "church": "temple",
    "mosque": "temple", "synagogue": "temple",
    # Beaches
    "beach": "beach", "surfing_area": "beach",
    # Nature
    "waterfall": "waterfall", "national_park": "nature_reserve",
    "park": "nature_reserve", "hiking_area": "nature_reserve",
    "campground": "nature_reserve", "scenic_viewpoint": "nature_reserve",
    "wildlife_park": "nature_reserve", "zoo": "nature_reserve",
    # Museums
    "museum": "museum", "art_gallery": "museum",
    "historical_landmark": "museum", "monument": "museum",
    "cultural_center": "museum", "performing_arts_theater": "museum",
    "art_studio": "museum",
    # Food & Beverage
    "restaurant": "restaurant", "food_court": "restaurant",
    "meal_delivery": "restaurant", "meal_takeaway": "restaurant",
    "cafe": "cafe", "coffee_shop": "cafe", "bakery": "cafe",
    "juice_bar": "cafe", "tea_house": "cafe", "ice_cream_shop": "cafe",
    "bar": "beach_club", "night_club": "beach_club",
    "pub": "beach_club", "liquor_store": "beach_club",
    # Wellness
    "spa": "spa_wellness", "wellness_center": "spa_wellness",
    "massage": "spa_wellness", "beauty_salon": "spa_wellness",
    "gym": "spa_wellness", "sports_complex": "spa_wellness",
    "yoga_studio": "yoga_studio", "pilates_studio": "yoga_studio",
    "meditation_center": "yoga_studio",
    # Markets
    "market": "market", "shopping_mall": "market",
    "supermarket": "market", "convenience_store": "market",
    "department_store": "market",
    # Work/Leisure
    "coworking_space": "coworking", "library": "coworking",
    "internet_cafe": "coworking", "business_center": "coworking",
    # Adventure
    "adventure_sports_center": "nature_reserve",
    "diving_center": "nature_reserve",
    "sports_activity_location": "nature_reserve",
    "tourist_attraction": "nature_reserve",
    # Fallback
    "point_of_interest": "TBD_TYPE", "establishment": "TBD_TYPE",
    "UNKNOWN": "TBD_TYPE",
}

# Namespace routing: given a Google type, which namespace does the place belong to?
GOOGLE_TYPE_TO_NAMESPACE: Dict[str, str] = {}
for _ns, _types in NAMESPACE_TYPES.items():
    for _t in _types:
        GOOGLE_TYPE_TO_NAMESPACE[_t] = _ns

PRICE_LEVEL_MAP: Dict[Optional[str], int] = {
    "PRICE_LEVEL_FREE": 1,
    "PRICE_LEVEL_INEXPENSIVE": 1,
    "PRICE_LEVEL_MODERATE": 2,
    "PRICE_LEVEL_EXPENSIVE": 3,
    "PRICE_LEVEL_VERY_EXPENSIVE": 4,
    None: -1,
    "": -1,
    "PRICE_LEVEL_UNSPECIFIED": -1,
}

POI_TYPE_TO_PRIMARY_CATEGORY: Dict[str, str] = {
    "temple": "Culture", "museum": "Culture",
    "beach": "Nature", "waterfall": "Nature", "nature_reserve": "Nature",
    "restaurant": "Food_Beverage", "cafe": "Food_Beverage",
    "beach_club": "Food_Beverage", "market": "Food_Beverage",
    "spa_wellness": "Wellness", "yoga_studio": "Wellness",
    "coworking": "Work_Leisure",
    "TBD_TYPE": "PENDING_CATEGORY",
}

# ─────────────────────────────────────────────────────────────────────────────
# POST-PROCESSING ENRICHMENT CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

ENRICHMENT_CONFIG = {
    "geocoding": {
        "enabled": True,
        "provider": "google_maps_reverse_geocode",
        "fields": ["district", "kelurahan_name", "kecamatan_name"],
        "rate_limit": 50,
    },
    "heuristics": {
        "is_indoor": {
            "indoor_types": ["shopping_mall", "museum", "art_gallery", "indoor_play_area"],
            "outdoor_types": ["beach", "hiking_area", "waterfall", "park"],
            "default": None,
        },
        "weather_resilience": {
            "rules": [
                {"condition": "indoor type", "value": "indoor"},
                {"condition": "cafe no outdoor", "value": "indoor"},
                {"condition": "cafe outdoor", "value": "covered"},
                {"condition": "beach/hiking", "value": "outdoor"},
            ],
            "default": "DATA_MISSING",
        },
        "transport_access": {
            "rules": [
                {"condition": "parking_options present", "value": "car"},
                {"condition": "hiking_area", "value": "hiking"},
                {"condition": "beach no parking", "value": "bike_only"},
            ],
            "default": "NOT_SET",
        },
    },
    "ml_inference": {
        "neighborhood_cluster": {
            "model": "kmeans_geo_vibe",
            "features": ["lat", "lng", "primary_type", "rating"],
            "n_clusters": 15,
        },
        "average_duration_minutes": {
            "model": "rule_based_lookup",
            "defaults": {
                "temple": 60, "museum": 90, "beach": 180,
                "waterfall": 120, "nature_reserve": 240,
                "restaurant": 90, "cafe": 60,
                "spa_wellness": 120, "yoga_studio": 90,
                "coworking": 300, "TBD_TYPE": 0,
            },
        },
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# DATA VALIDATION & QUALITY THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────

QUALITY_THRESHOLDS = {
    "min_rating": 0,       # No filtering — collect everything, filter later
    "min_reviews": 0,      # No filtering — collect everything, filter later
    "max_age_days": 365,
    "coordinate_precision": 6,
}