# System Metadata in Vectorspaces

```json
{
  "system_metadata_version": "3.0",
  "project_name": "SobatNavi",
  "enum_library": {
    "PoiType": ["temple", "beach", "waterfall", "nature_reserve", "museum", "restaurant", "cafe", "beach_club", "spa_wellness", "market", "coworking", "yoga_studio"],
    "PrimaryCategory": ["Culture", "Nature", "Adventure", "Wellness", "Food_Beverage", "Work_Leisure"],
    "AtmosphereVibe": ["quiet", "lively", "romantic", "family_friendly", "traditional", "modern", "hidden_gem"],
    "PriceTier": [1, 2, 3, 4],
    "AdatPolicy": ["allowed", "restricted", "forbidden"],
    "MenstruationPolicy": ["strict_no_entry", "no_restriction"],
    "DressCode": ["none", "sarung_and_sash", "modest"],
    "WeatherResilience": ["indoor", "covered", "outdoor"],
    "TransportAccess": ["car", "bike_only", "hiking"],
    "ImpactLevel": ["low", "medium", "high", "extreme"],
    "Persona": ["Backpacker", "Digital Nomad", "Spiritual Seeker", "Luxury Traveler", "Family", "Adventure Junkie"],
    "Lighting": ["sunrise", "golden_hour", "daylight", "sunset", "night"],
    "District": ["Kuta", "Seminyak", "Ubud", "Canggu", "Uluwatu", "Nusa Dua", "Sanur", "Kintamani", "Bedugul", "Amed"]
  },
  "namespaces": {
    "poi_attractions": {
      "poi_id": "UUID",
      "name": "String",
      "poi_type": "Enum(PoiType)",
      "geospatial": {
        "latitude": "Float",
        "longitude": "Float",
        "district": "Enum(District)",
        "kelurahan_name": "String",
        "neighborhood_cluster": "String (e.g., jungle_ubud)"
      },
      "categorization": {
        "primary_category": "Enum(PrimaryCategory)",
        "price_tier": "Enum(PriceTier)",
        "is_indoor": "Boolean",
        "weather_resilience": "Enum(WeatherResilience)",
        "rating_average": "Float (0.0-5.0)",
        "popularity_score": "Float (0.0-1.0)"
      },
      "operational": {
        "opening_hours": "String (HH:mm-HH:mm)",
        "average_duration_minutes": "Integer",
        "closed_dates": "List[ISO_Date]",
        "transport_access": "Enum(TransportAccess)"
      }
    },
    "culinary_amenities": {
	  "poi_id": "UUID",
	  "name": "String",
	  "cuisine_type": "String",
	  "geospatial": {
	    "latitude": "Float",
	    "longitude": "Float",
	    "kelurahan_name": "String"
	  },
	  "attributes": {
	    "dietary_options": "List[String]",
	    "price_tier": "Enum(PriceTier)",
	    "is_work_friendly": "Boolean"
	  },
	  "operational": {
	    "opening_hours": "String (HH:mm-HH:mm)",
	    "average_meal_duration_minutes": "Integer"
	  }
	},
    "cultural_context_adat": {
      "poi_reference_id": "UUID",
      "constraints": {
        "is_sacred_site": "Boolean",
        "menstruation_policy": "Enum(MenstruationPolicy)",
        "dress_code_requirement": "Enum(DressCode)",
        "ceremony_blackout_dates": "List[ISO_Date]"
      },
      "guidance": {
        "etiquette_short_note": "String",
        "alternative_poi_reference_id": "UUID",
        "cultural_reasoning": "String"
      }
    },
    "visual_vibe_embeddings": {
      "poi_reference_id": "UUID",
      "vibe_identity": {
        "primary_theme": "String (e.g., Jungle Zen)",
        "aesthetic_score": "Float (0.0-1.0)",
        "aesthetic_tags": "List[String]",
        "suitable_personas": "List[Enum(Persona)]"
      },
      "photography_logic": {
        "best_lighting_time": "Enum(Lighting)",
        "quiet_hour_window": "String (HH:mm-HH:mm)"
      },
      "vector_storage_reference": "String (Vector_ID)"
    },
    "dynamic_kelurahan_events": {
      "event_id": "UUID",
      "event_type": "String (e.g., road_closure|ceremony)",
      "geospatial": {
        "kelurahan_id": "String",
        "affected_poi_ids": "List[UUID]",
        "affected_road_name": "String"
      },
      "temporal": {
        "start_time": "ISO_Datetime",
        "end_time": "ISO_Datetime"
      },
      "impact": {
        "traffic_delay_level": "Enum(ImpactLevel)",
        "accessibility_status": "String (closed|limited|normal)"
      }
    },
    "inspiration_narration": {
      "poi_reference_id": "UUID",
      "narration_content": {
        "vibe_category": "Enum(AtmosphereVibe)",
        "story_hook": "String",
        "historical_fact": "String"
      },
      "matching_logic": {
        "target_personas": "List[Enum(Persona)]",
        "search_keywords": "List[String]"
      }
    },
    "accommodation_anchors": {
      "hotel_id": "UUID",
      "identity": {
        "name": "String",
        "accommodation_type": "String",
        "vibe_description": "List[Enum(AtmosphereVibe)]"
      },
      "logistics": {
        "latitude": "Float",
        "longitude": "Float",
        "neighborhood_cluster": "String",
        "check_in_out_time": "List[String (HH:mm)]"
      },
      "amenities": {
        "wifi_speed_level": "String (low|medium|high)",
        "workspace_available": "Boolean"
      }
    }
  }
}
```



# Vector Namespaces

### 1. Namespace: `poi_attractions` (The Core)

**Fungsi:** Data utama destinasi wisata untuk _routing_, _batching_, dan _time-slicing_.

Metadata example:
```json
{
  "poi_id": "uuid",
  "name": "Pura Tirta Empul",
  "poi_type": "temple",
  "geospatial": {
    "latitude": -8.412,
    "longitude": 115.287,
    "district": "Ubud",
    "kelurahan_name": "Manukaya",
    "neighborhood_cluster": "ubud_nature"
  },
  "categorization": {
    "primary_category": "Culture",
    "price_tier": 2,
    "is_indoor": false,
    "rating_average": 4.8,
    "popularity_score": 0.95
  },
  "operational": {
    "opening_hours": "08:00-18:00",
    "average_duration_minutes": 90,
    "closed_dates": ["2026-03-07", "2026-03-08"]
  }
}
```

### 2. Namespace: `culinary_amenities` (The Slot Filler)

**Fungsi:** Mengisi slot makan siang/malam dalam itinerary tanpa mengganggu kuota tempat wisata.

Metadata example:
```json
{
  "poi_id": "uuid",
  "name": "Nasi Ayam Kedewatan Ibu Mangku",
  "cuisine_type": "Balinese",
  "geospatial": {
    "latitude": -8.125,
    "longitude": 115.125,
    "kelurahan_name": "Kedewatan"
  },
  "attributes": {
    "dietary_options": ["halal_friendly", "vegetarian"],
    "price_tier": 1,
    "is_work_friendly": false
  },
  "operational": {
    "opening_hours": "08:00-18:00",
    "average_meal_duration_minutes": 45
  }
}
```

### 3. Namespace: `cultural_context_adat` (The Gatekeeper)

**Fungsi:** Validasi aturan adat dan kebijakan menstruasi (_Hard Constraint_).

Metadata example:
```json
{
  "poi_reference_id": "uuid",
  "constraints": {
    "is_sacred_site": true,
    "menstruation_policy": "forbidden",
    "dress_code_requirement": "sarung_and_sash",
    "ceremony_blackout_dates": ["2026-03-07", "2026-03-08"]
  },
  "guidance": {
    "etiquette_short_note": "Gunakan sarung, jaga kesopanan saat ada upacara.",
    "alternative_poi_reference_id": "uuid_of_nearby_temple",
    "cultural_reasoning": "Pura Kahyangan Jagat dengan aturan adat ketat."
  }
}
```

### 4. Namespace: `visual_vibe_embeddings` (The Mood Matcher)

**Fungsi:** Pencarian berbasis estetika visual (_Semantic Aesthetic_).

Metadata example:
```json
{
  "poi_reference_id": "uuid",
  "vibe_identity": {
    "primary_theme": "Jungle Zen",
    "aesthetic_score": 0.95,
    "aesthetic_tags": ["peaceful", "lush", "minimalist"],
    "suitable_personas": ["Digital Nomad", "Wellness Traveler"]
  },
  "photography_logic": {
    "best_lighting_time": "golden_hour",
    "quiet_hour_window": "06:00-08:00"
  },
  "vector_storage_reference": "vector_id_001"
}
```

### 5. Namespace: `dynamic_kelurahan_events` (CRAG Fallback)

**Fungsi:** Data _real-time_ untuk pemicu **Corrective RAG** jika ada penutupan jalan atau upacara.

Metadata example:
```json
{
  "event_id": "uuid",
  "event_type": "road_closure",
  "geospatial": {
    "kelurahan_id": "KLD-UBD-001",
    "affected_poi_ids": ["uuid1", "uuid2"],
    "affected_road_name": "Jalan Raya Ubud"
  },
  "temporal": {
    "start_time": "2026-03-07T06:00:00",
    "end_time": "2026-03-07T18:00:00"
  },
  "impact": {
    "traffic_delay_level": "high",
    "accessibility_status": "closed"
  }
}
```

### 6. Namespace: `inspiration_narration` (Heidi's Voice)

**Fungsi:** Cerita unik untuk memicu interaksi (_Co-creation_).

Metadata example:
```json
{
  "poi_reference_id": "uuid",
  "narration_content": {
    "vibe_category": "lively",
    "story_hook": "Pura ini ditemukan pada abad ke-11 dengan mata air suci yang dipercaya menyembuhkan.",
    "historical_fact": "Mpu Kuturan membangun situs ini untuk menjaga energi Bali."
  },
  "matching_logic": {
    "target_personas": ["Spiritual Seeker", "History Buff"],
    "search_keywords": ["authentic", "hidden", "ancient"]
  }
}
```


### 7. Namespace: `accommodation_anchors` (The Basecamp)

**Fungsi:** Titik acuan logistik, batasan anggaran harian, dan preferensi kenyamanan.

Metadata example:
```json
{
  "hotel_id": "uuid",
  "identity": {
    "name": "Alila Ubud",
    "accommodation_type": "5-star Resort",
    "vibe_description": ["quiet", "romantic"]
  },
  "logistics": {
    "latitude": -8.123,
    "longitude": 115.123,
    "neighborhood_cluster": "jungle_ubud",
    "check_in_out_time": ["14:00", "12:00"]
  },
  "amenities": {
    "wifi_speed_level": "high",
    "workspace_available": true
  }
}
```