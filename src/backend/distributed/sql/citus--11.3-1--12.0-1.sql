-- bump version to 12.0-1

CREATE TABLE citus.pg_dist_schema (
    schemaid oid NOT NULL,
    colocationid int NOT NULL,
    CONSTRAINT pg_dist_schema_pkey PRIMARY KEY (schemaid),
    CONSTRAINT pg_dist_schema_unique_colocationid_index UNIQUE (colocationid)
);

ALTER TABLE citus.pg_dist_schema SET SCHEMA pg_catalog;

GRANT SELECT ON pg_catalog.pg_dist_schema TO public;

-- udfs used to modify pg_dist_schema on workers, to sync metadata
#include "udfs/citus_internal_add_tenant_schema/12.0-1.sql"
#include "udfs/citus_internal_delete_tenant_schema/12.0-1.sql"

#include "udfs/citus_prepare_pg_upgrade/12.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/12.0-1.sql"

-- udfs used to modify pg_dist_schema globally via drop trigger
#include "udfs/citus_internal_unregister_tenant_schema_globally/12.0-1.sql"
#include "udfs/citus_drop_trigger/12.0-1.sql"

DROP VIEW citus_shards;
DROP VIEW IF EXISTS pg_catalog.citus_tables;
DROP VIEW IF EXISTS public.citus_tables;
DROP FUNCTION citus_shard_sizes;
#include "udfs/citus_shard_sizes/12.0-1.sql"

#include "udfs/citus_tables/12.0-1.sql"
#include "udfs/citus_shards/12.0-1.sql"

#include "udfs/citus_schemas/12.0-1.sql"

-- udfs used to include schema-based tenants in tenant monitoring
#include "udfs/citus_stat_tenants_local/12.0-1.sql"

-- udfs to convert a regular/tenant schema to a tenant/regular schema
#include "udfs/citus_schema_distribute/12.0-1.sql"
#include "udfs/citus_schema_undistribute/12.0-1.sql"

#include "udfs/drop_old_time_partitions/12.0-1.sql"
#include "udfs/get_missing_time_partition_ranges/12.0-1.sql"

-- Update the default rebalance strategy to 'by_disk_size', but only if the
-- default is currently 'by_shard_count'
SELECT citus_set_default_rebalance_strategy(name)
FROM pg_dist_rebalance_strategy
WHERE name = 'by_disk_size'
    AND (SELECT default_strategy FROM pg_dist_rebalance_strategy WHERE name = 'by_shard_count');

CREATE SCHEMA citus_catalog;
GRANT USAGE ON SCHEMA citus_catalog TO public;

CREATE TABLE citus_catalog.database_shard (
	database_oid oid not null,
	node_group_id int not null,
	is_available bool not null,
	PRIMARY KEY (database_oid)
);

/*
 * execute_command_on_all_nodes runs a command on all nodes
 * in a 2PC.
 */
CREATE OR REPLACE FUNCTION pg_catalog.execute_command_on_all_nodes(
    command text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$execute_command_on_all_nodes$$;
COMMENT ON FUNCTION pg_catalog.execute_command_on_all_nodes(text) IS
 'run a command on all other nodes in a 2PC';

/*
 * execute_command_on_other_nodes runs a command on all other nodes
 * in a 2PC.
 */
CREATE OR REPLACE FUNCTION pg_catalog.execute_command_on_other_nodes(
    command text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$execute_command_on_other_nodes$$;
COMMENT ON FUNCTION pg_catalog.execute_command_on_other_nodes(text) IS
 'run a command on all other nodes in a 2PC';

/*
 * database_shard_assign assigns a database to a specific shard.
 */
CREATE OR REPLACE FUNCTION pg_catalog.database_shard_assign(database_name text)
 RETURNS int
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$database_shard_assign$$;
COMMENT ON FUNCTION pg_catalog.database_shard_assign(text) IS
 'run a command on all other nodes in a 2PC';
