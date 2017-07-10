DROP TABLE IF EXISTS bordeaux_metropole_elements;
DROP TABLE IF EXISTS bordeaux_metropole_geomelements;
CREATE TABLE bordeaux_metropole_elements(
       elem varchar,
       osm_id bigint,
       first_at timestamp,
       last_at timestamp,
       lifespan float,
       n_activity_days float,
       version int,
       n_chgsets int,
       n_users int,
       n_autocorrs int,
       n_corrs int,
       visible boolean,
       first_user int,
       last_user int,
       first_user_group int,
       last_user_group int
);

COPY bordeaux_metropole_elements FROM '/home/rde/data/osm-history/output-extracts/bordeaux-metropole/bordeaux-metropole-elem-md.csv' WITH(FORMAT CSV, HEADER, QUOTE '"');


SELECT l.osm_id, h.first_at, h.lifespan, h.lifecycle, h.n_activity_days,
h.version, h.visible,
h.first_user_group, h.last_user_group,
h.n_users, h.n_chgsets, h.n_autocorrs, h.n_corrs, l.way
INTO bordeaux_metropole_geomelements
FROM bordeaux_metropole_elements as h
INNER JOIN bordeaux_metropole_line as l
ON h.osm_id = l.osm_id 
WHERE l.highway IS NOT NULL AND h.elem = 'way'
ORDER BY l.osm_id;
