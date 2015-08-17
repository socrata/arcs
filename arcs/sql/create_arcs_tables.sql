CREATE TABLE IF NOT EXISTS arcs_group (
  id SERIAL NOT NULL,
  created_at timestamp with time zone NOT NULL,
  description text,
  stats json,
  UNIQUE (id)
);

CREATE TABLE IF NOT EXISTS arcs_job (
  id SERIAL NOT NULL,
  external_id text NOT NULL,
  created_at timestamp with time zone NOT NULL,
  completed_at timestamp with time zone,
  platform text,
  job_type text,
  metadata json,
  results json, -- raw results data
  UNIQUE (id),
  UNIQUE (external_id)
);

CREATE TABLE IF NOT EXISTS arcs_query_result (
  id serial NOT NULL,
  query text NOT NULL,
  result_fxf text NOT NULL,
  judgment real,
  job_id integer REFERENCES arcs_job (id), -- for tracking source of judgment
  UNIQUE (query, result_fxf),
  UNIQUE (id)
);

CREATE INDEX ON arcs_query_result(query, result_fxf, judgment);

CREATE TABLE IF NOT EXISTS arcs_group_join (
  group_id integer REFERENCES arcs_group (id),
  query_result_id integer REFERENCES arcs_query_result(id),
  res_position integer CHECK (res_position >= 0),
  payload json, -- the fields needed for the CSV we upload
  UNIQUE (group_id, query_result_id, res_position)
);

CREATE TABLE IF NOT EXISTS arcs_experiment (
  id SERIAL NOT NULL,
  created_at timestamp with time zone NOT NULL,
  baseline_group_id integer REFERENCES arcs_group (id),
  experimental_group_id integer REFERENCES arcs_group (id),
  stats json,
  UNIQUE (id)
);
