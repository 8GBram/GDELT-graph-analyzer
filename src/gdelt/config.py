import polars as pl

GDELT_SCHEMA = {
    # Event ID and dates
    "id": pl.String,
    "raw_date": pl.String,
    "month_year": pl.Int32,
    "year": pl.Int32,
    "fraction_date": pl.Float64,

    # Actor1
    "actor1_code": pl.String,
    "actor1_name": pl.String,
    "actor1_country_code": pl.String,
    "actor1_known_group_code": pl.String,
    "actor1_ethnic_code": pl.String,
    "actor1_religion1_code": pl.String,
    "actor1_religion2_code": pl.String,
    "actor1_type1_code": pl.String,
    "actor1_type2_code": pl.String,
    "actor1_type3_code": pl.String,

    # Actor2
    "actor2_code": pl.String,
    "actor2_name": pl.String,
    "actor2_country_code": pl.String,
    "actor2_known_group_code": pl.String,
    "actor2_ethnic_code": pl.String,
    "actor2_religion1_code": pl.String,
    "actor2_religion2_code": pl.String,
    "actor2_type1_code": pl.String,
    "actor2_type2_code": pl.String,
    "actor2_type3_code": pl.String,

    # Event Properties
    "is_root_event": pl.Int8,
    "event_code": pl.String,       # Keeps leading zeros (e.g., "010")
    "event_base_code": pl.String,  # Keeps leading zeros
    "event_root_code": pl.String,  # Keeps leading zeros
    "quad_class": pl.Int32,
    "goldstein_scale": pl.Float64,
    "num_mentions": pl.Int32,
    "num_sources": pl.Int32,
    "num_articles": pl.Int32,
    "avg_tone": pl.Float64,

    # Actor1 geography
    "actor1_geo_type": pl.Int32,
    "actor1_geo_fullname": pl.String,
    "actor1_geo_country_code": pl.String,
    "actor1_geo_adm1_code": pl.String,
    "actor1_geo_adm2_code": pl.String,
    "actor1_geo_lat": pl.Float64,
    "actor1_geo_long": pl.Float64,
    "actor1_geo_feature_id": pl.String,

    # Actor2 geography
    "actor2_geo_type": pl.Int32,
    "actor2_geo_fullname": pl.String,
    "actor2_geo_country_code": pl.String,
    "actor2_geo_adm1_code": pl.String,
    "actor2_geo_adm2_code": pl.String,
    "actor2_geo_lat": pl.Float64,
    "actor2_geo_long": pl.Float64,
    "actor2_geo_feature_id": pl.String,

    # Where the event happened
    "action_geo_type": pl.Int32,
    "action_geo_fullname": pl.String,
    "action_geo_country_code": pl.String,
    "action_geo_adm1_code": pl.String,
    "action_geo_adm2_code": pl.String,
    "action_geo_lat": pl.Float64,
    "action_geo_long": pl.Float64,
    "action_geo_feature_id": pl.String,

    # Provenance
    "date_added": pl.Int64,
    "source_url": pl.String,
}

assert len(GDELT_SCHEMA) == 61