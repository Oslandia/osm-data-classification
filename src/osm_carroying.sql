CREATE INDEX insee_geom_gist
ON open_data.insee_200_carreau USING GIST(wkb_geometry);
CREATE INDEX osm_geom_gist
ON bordeaux_metropole_geomelements USING GIST(way);

DROP TABLE IF EXISTS bordeaux_metropole_carroyed_ways;
CREATE TABLE bordeaux_metropole_carroyed_ways AS (
SELECT insee.ogc_fid, count(*) AS nb_ways,
avg(bm.version) AS avg_version, avg(bm.lifecycle) AS avg_lifecycle,
avg(bm.n_users) AS avg_n_users, avg(bm.n_chgsets) AS avg_n_chgsets,
avg(bm.n_corrs) AS avg_n_corrs, avg(bm.n_autocorrs) AS avg_n_autocorrs,
insee.wkb_geometry AS geom
FROM open_data.insee_200_carreau AS insee
JOIN bordeaux_metropole_geomelements AS bm
ON ST_Intersects(insee.wkb_geometry, bm.way)
GROUP BY insee.ogc_fid
);
