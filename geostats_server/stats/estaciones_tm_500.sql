SELECT COUNT(*) AS point_count
FROM estaciones_tm et 
WHERE ST_DWithin(
    geography(ST_SetSRID(ST_Point(:lng, :lat), 4326)),
    geography(ST_Transform(geom, 4326)),
    500
);
