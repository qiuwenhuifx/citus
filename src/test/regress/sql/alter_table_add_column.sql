CREATE SCHEMA alter_table_add_column;
SET search_path TO alter_table_add_column;

SET citus.next_shard_id TO 1830000;
SET citus.shard_replication_factor TO 1;

SET client_min_messages TO NOTICE;

CREATE TABLE referenced (int_col integer PRIMARY KEY);
CREATE TABLE referencing (text_col text);
SELECT create_distributed_table('referenced', null);
SELECT create_distributed_table('referencing', null);

CREATE OR REPLACE FUNCTION my_random(numeric)
  RETURNS numeric AS
$$
BEGIN
  RETURN 7 * $1;
END;
$$
LANGUAGE plpgsql IMMUTABLE;

CREATE COLLATION caseinsensitive (
	provider = icu,
	locale = 'und-u-ks-level2'
);

CREATE TYPE "simple_!\'custom_type" AS (a integer, b integer);

ALTER TABLE referencing ADD COLUMN test_1 integer DEFAULT (my_random(7) + random() + 5) NOT NULL CONSTRAINT fkey REFERENCES referenced(int_col) ON UPDATE SET DEFAULT ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE referencing ADD COLUMN test_2 integer UNIQUE REFERENCES referenced(int_col) ON UPDATE CASCADE ON DELETE SET DEFAULT NOT DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE referencing ADD COLUMN test_3 integer GENERATED ALWAYS AS (test_1 * my_random(1)) STORED UNIQUE REFERENCES referenced(int_col) MATCH FULL;
ALTER TABLE referencing ADD COLUMN test_4 integer PRIMARY KEY WITH (fillfactor=70) NOT NULL REFERENCES referenced(int_col) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET DEFAULT;
ALTER TABLE referencing ADD COLUMN test_5 integer CONSTRAINT unique_c UNIQUE WITH (fillfactor=50);
ALTER TABLE referencing ADD COLUMN test_6 text COMPRESSION pglz COLLATE caseinsensitive NOT NULL;
ALTER TABLE referencing ADD COLUMN "test_\'!7" "simple_!\'custom_type";

-- adding check constraints by using ADD COLUMN is not supported
ALTER TABLE referencing ADD COLUMN test_8 integer CHECK (test_8 > 0);

-- try to add test_6 again, but with IF NOT EXISTS
ALTER TABLE referencing ADD COLUMN IF NOT EXISTS test_6 text;
ALTER TABLE referencing ADD COLUMN IF NOT EXISTS test_6 integer;

SELECT (groupid = 0) AS is_coordinator, result FROM run_command_on_all_nodes(
  $$SELECT get_grouped_fkey_constraints FROM get_grouped_fkey_constraints('alter_table_add_column.referencing')$$
)
JOIN pg_dist_node USING (nodeid)
ORDER BY is_coordinator DESC, result;

SELECT (groupid = 0) AS is_coordinator, result FROM run_command_on_all_nodes(
  $$SELECT get_index_defs FROM get_index_defs('alter_table_add_column', 'referencing')$$
)
JOIN pg_dist_node USING (nodeid)
ORDER BY is_coordinator DESC, result;

SELECT (groupid = 0) AS is_coordinator, result FROM run_command_on_all_nodes(
  $$SELECT get_column_defaults FROM get_column_defaults('alter_table_add_column', 'referencing')$$
)
JOIN pg_dist_node USING (nodeid)
ORDER BY is_coordinator DESC, result;

SELECT (groupid = 0) AS is_coordinator, result FROM run_command_on_all_nodes(
  $$SELECT get_column_attrs FROM get_column_attrs('alter_table_add_column.referencing')$$
)
JOIN pg_dist_node USING (nodeid)
ORDER BY is_coordinator DESC, result;

SET client_min_messages TO WARNING;
DROP SCHEMA alter_table_add_column CASCADE;
