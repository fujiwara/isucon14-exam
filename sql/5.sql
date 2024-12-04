ALTER TABLE chairs
  ADD COLUMN latitude INTEGER DEFAULT NULL,
  ADD COLUMN longitude INTEGER NULL DEFAULT NULL;

UPDATE chairs SET
    latitude = (SELECT latitude FROM chair_locations WHERE chair_locations.chair_id = chairs.id ORDER BY created_at DESC LIMIT 1),
    longitude = (SELECT longitude FROM chair_locations WHERE chair_locations.chair_id = chairs.id ORDER BY created_at DESC LIMIT 1);
