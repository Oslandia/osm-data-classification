DROP TABLE IF EXISTS bordeaux_metropole_elements;
DROP TABLE IF EXISTS bordeaux_metropole_geomelements;
CREATE TABLE bordeaux_metropole_elements(
       id int,
       elem varchar,
       osm_id bigint,
       first_at timestamp,
       last_at timestamp,
       lifecycle float,
       version int,
       n_chgsets int,
       n_users int,
       n_autocorrs int,
       n_corrs int,
       visible boolean,
       ntags int,
       tagkeys varchar
);

COPY bordeaux_metropole_elements FROM '/home/rde/data/osm-history/output-extracts/bordeaux-metropole/bordeaux-metropole-elem-md.csv' WITH(FORMAT CSV, HEADER, QUOTE '"');

SELECT l.osm_id, h.first_at, h.lifecycle, h.version, h.visible,
h.ntags, h.tagkeys, 
h.n_users, h.n_chgsets, h.n_autocorrs, h.n_corrs, l.way
INTO bordeaux_metropole_geomelements
FROM bordeaux_metropole_elements as h
INNER JOIN bordeaux_metropole_line as l
ON h.osm_id = l.osm_id 
WHERE l.highway IS NOT NULL AND h.elem = 'way'
ORDER BY l.osm_id;
