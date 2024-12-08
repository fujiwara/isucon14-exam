ALTER TABLE chairs
  ADD COLUMN total_distance INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN moved_at DATETIME(6) DEFAULT NULL,
  ADD COLUMN latitude INTEGER DEFAULT NULL,
  ADD COLUMN longitude INTEGER NULL DEFAULT NULL;

CREATE OR REPLACE VIEW distances AS SELECT chair_id,
                          SUM(IFNULL(distance, 0)) AS total_distance,
                          MAX(created_at)          AS total_distance_updated_at
                   FROM (SELECT chair_id,
                                created_at,
                                ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at)) +
                                ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at)) AS distance
                         FROM chair_locations) tmp
                   GROUP BY chair_id;

UPDATE chairs
JOIN distances ON distances.chair_id = chairs.id
SET chairs.total_distance = distances.total_distance,
    chairs.updated_at = distances.total_distance_updated_at;

UPDATE chairs SET
    latitude = (SELECT latitude FROM chair_locations WHERE chair_locations.chair_id = chairs.id ORDER BY created_at DESC LIMIT 1),
    longitude = (SELECT longitude FROM chair_locations WHERE chair_locations.chair_id = chairs.id ORDER BY created_at DESC LIMIT 1),
    moved_at = (SELECT created_at FROM chair_locations WHERE chair_locations.chair_id = chairs.id ORDER BY created_at DESC LIMIT 1);

ALTER TABLE ride_statuses DROP COLUMN id;
ALTER TABLE ride_statuses DROP COLUMN chair_sent_at;
ALTER TABLE ride_statuses DROP COLUMN app_sent_at;

DROP TABLE IF EXISTS ride_status;
CREATE TABLE ride_status
(
  ride_id VARCHAR(26)                                                                        NOT NULL COMMENT 'ライドID',
  status          ENUM ('MATCHING', 'ENROUTE', 'PICKUP', 'CARRYING', 'ARRIVED', 'COMPLETED') NOT NULL COMMENT '状態',
  updated_at      DATETIME(6)                                                                NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '状態変更日時',
  PRIMARY KEY (ride_id, status, updated_at)
)
  COMMENT = 'ライドステータスの変更履歴テーブル';

INSERT INTO ride_status
SELECT 
    t1.ride_id,
    t1.status,
    t1.created_at
FROM 
    ride_statuses t1
JOIN (
    SELECT 
        ride_id,
        MAX(created_at) AS latest_created_at
    FROM 
        ride_statuses
    GROUP BY 
        ride_id
) t2
ON 
    t1.ride_id = t2.ride_id 
    AND t1.created_at = t2.latest_created_at;

DROP TABLE ride_statuses;
