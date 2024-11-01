/*Schema/Table Creation*/
DROP SCHEMA IF EXISTS source CASCADE;
DROP SCHEMA IF EXISTS target CASCADE;
CREATE SCHEMA source;
CREATE SCHEMA target;
ALTER DATABASE airflow SET timezone TO 'Europe/Oslo';

/*Trigger function*/
CREATE OR REPLACE FUNCTION updated_at_function()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at= now(); 
   RETURN NEW;
END;
$$ language 'plpgsql';

/*Table creation - Source*/
CREATE TABLE IF NOT EXISTS source.biler (
    id serial,
    uuid varchar(128),
    radar_id integer,
    license_plate varchar(8),
    vehicle_make varchar(100),
    vehicle_model varchar(100),
    vehicle_color varchar(50),
    velocity integer,
    velocity_limit integer,
    country_code varchar(2),
    state_name varchar(100),
    created_at timestamp default NOW(),
    updated_at timestamp default NOW(),
    sourceTime bigint default ROUND(EXTRACT(EPOCH FROM NOW())),
    CONSTRAINT biler_pk PRIMARY KEY (id)
);

/*Table creation - Target*/
CREATE TABLE IF NOT EXISTS target.biler (
    id serial,
    uuid varchar(128),
    radar_id integer,
    license_plate varchar(8),
    vehicle_make varchar(100),
    vehicle_model varchar(100),
    vehicle_color varchar(50),
    velocity integer,
    velocity_limit integer,
    country_code varchar(2),
    state_name varchar(100),
    created_at timestamp default NOW(),
    updated_at timestamp default NOW(),
    sourceTime bigint default ROUND(EXTRACT(EPOCH FROM NOW())),
    CONSTRAINT biler_pk PRIMARY KEY (id)
);

/*Triggers to upsert with updated_at*/
CREATE TRIGGER updated_at_trigger_source BEFORE UPDATE
ON source.biler FOR EACH ROW EXECUTE PROCEDURE 
updated_at_function();

CREATE TRIGGER updated_at_trigger_target BEFORE UPDATE
ON target.biler FOR EACH ROW EXECUTE PROCEDURE 
updated_at_function();
