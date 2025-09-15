CREATE TABLE machines (machine_id SERIAL PRIMARY KEY, name TEXT NOT NULL, area TEXT);
CREATE TABLE sensors (
  sensor_id SERIAL PRIMARY KEY, machine_id INT REFERENCES machines(machine_id),
  name TEXT NOT NULL, unit TEXT, description TEXT
);
CREATE TABLE telemetry (
  ts TIMESTAMPTZ NOT NULL, sensor_id INT REFERENCES sensors(sensor_id),
  value DOUBLE PRECISION NOT NULL, PRIMARY KEY (ts, sensor_id)
);
CREATE TABLE lab_samples (
  sample_id SERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL,
  machine_id INT REFERENCES machines(machine_id),
  tailings_cr2o3_pct DOUBLE PRECISION
);
