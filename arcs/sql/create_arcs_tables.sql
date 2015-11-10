CREATE TABLE IF NOT EXISTS arcs_group (
  id SERIAL NOT NULL,
  created_at timestamp with time zone NOT NULL,
  name text DEFAULT 'baseline',
  description text,
  params json,
  stats json,
  raw json,
  group_type text,
  UNIQUE (id)
);

CREATE TABLE IF NOT EXISTS arcs_job (
  id SERIAL NOT NULL,
  external_id text NOT NULL,
  created_at timestamp with time zone NOT NULL,
  completed_at timestamp with time zone,
  platform text,
  metadata json,
  results json, -- raw results data
  UNIQUE (id),
  UNIQUE (external_id)
);

CREATE TABLE IF NOT EXISTS arcs_query (
  id SERIAL NOT NULL,
  query text NOT NULL,
  domain text,
  UNIQUE (id),
  UNIQUE (query, domain)
);

CREATE TABLE IF NOT EXISTS arcs_query_group_join (
  query_id integer REFERENCES arcs_query (id),
  group_id integer REFERENCES arcs_group (id),
  UNIQUE (query_id, group_id)
);

CREATE INDEX ON arcs_query_group_join(group_id);

CREATE TABLE IF NOT EXISTS arcs_query_result (
  id serial NOT NULL,
  query text NOT NULL,
  result_fxf text NOT NULL,
  judgment real,
  is_gold bool DEFAULT FALSE,
  raw_judgments json,
  job_id integer REFERENCES arcs_job (id), -- for tracking source of judgment
  query_id integer REFERENCES arcs_query (id),
  UNIQUE (query, result_fxf),
  UNIQUE (id)
);

CREATE INDEX ON arcs_query_result(query, result_fxf, judgment);

CREATE TABLE IF NOT EXISTS arcs_group_join (
  group_id integer REFERENCES arcs_group (id),
  query_result_id integer REFERENCES arcs_query_result (id),
  result_position integer CHECK (result_position >= 0),
  UNIQUE (group_id, query_result_id, result_position)
);

CREATE INDEX ON arcs_group_join(group_id);

CREATE TABLE IF NOT EXISTS arcs_experiment (
  id SERIAL NOT NULL,
  created_at timestamp with time zone NOT NULL,
  baseline_group_id integer REFERENCES arcs_group (id),
  experimental_group_id integer REFERENCES arcs_group (id),
  stats json,
  UNIQUE (id)
);

CREATE TABLE IF NOT EXISTS arcs_measurement_set(
  id SERIAL NOT NULL,
  created_at timestamp with time zone NOT NULL,
  active bool DEFAULT FALSE,
  UNIQUE (id)
);

CREATE TABLE IF NOT EXISTS arcs_query_measurement_set_join(
  query_id integer REFERENCES arcs_query (id),
  measurement_set_id integer REFERENCES arcs_measurement_set (id),
  UNIQUE (query_id, measurement_set_id)
);
