SELECT COUNT(*) AS total_count
FROM (
    SELECT fclass
    FROM gis_osm_pois_free_1
    WHERE fclass IN ('convenience', 'supermarket', 'market_place')
      AND ST_DWithin(
          geography(geom),
          geography(ST_SetSRID(ST_Point(:lng, :lat), 4326)),
          200
      )
    UNION ALL
    SELECT fclass
    FROM gis_osm_pois_a_free_1
    WHERE fclass IN ('convenience', 'supermarket', 'market_place')
      AND ST_DWithin(
          geography(ST_Centroid(geom)),
          geography(ST_SetSRID(ST_Point(:lng, :lat), 4326)),
          200
      )
) AS combined;
