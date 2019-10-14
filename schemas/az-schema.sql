CREATE SCHEMA IF NOT EXISTS az;

DROP TABLE IF EXISTS az.raw_ratings CASCADE;
CREATE TABLE az.raw_ratings (
  user_key VARCHAR NOT NULL,
  asin VARCHAR NOT NULL,
  rating REAL NOT NULL,
  rating_time BIGINT NOT NULL
);

INSERT INTO stage_dep (stage_name, dep_name, dep_key)
SELECT 'ag-schema', stage_name, stage_key
FROM stage_status
WHERE stage_name = 'common-schema';
