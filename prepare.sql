CREATE TABLE parking_session (
  id               integer unique not null,
  accountid        integer,
  start_time_mls   bigint,
  end_time_mls     bigint,
  zone_number      varchar(10)
);

\copy parking_session FROM 'path/to/your/database.csv' HEADER DELIMITER ';' CSV;

ALTER TABLE parking_session
  ADD COLUMN start_time timestamp;
ALTER TABLE parking_session
  ADD COLUMN end_time timestamp;

UPDATE parking_session
  SET
    start_time = to_timestamp(start_time_mls::double precision / 1000),
    end_time = to_timestamp(end_time_mls::double precision / 1000);

ALTER TABLE parking_session
  DROP start_time_mls,
  DROP end_time_mls;

CREATE INDEX ON parking_session(date(start_time));
ANALYZE parking_session;
