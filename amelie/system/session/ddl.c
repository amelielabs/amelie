
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>
#include <amelie_frontend.h>
#include <amelie_session.h>

static void
ddl_create_schema(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_schema_create_of(stmt->ast);
	schema_mgr_create(&self->share->db->schema_mgr, tr, arg->config,
	                  arg->if_not_exists);
}

static void
ddl_drop_schema(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_schema_drop_of(stmt->ast);
	cascade_schema_drop(self->share->db, tr, &arg->name->string, arg->cascade,
	                    arg->if_exists);
}

static void
ddl_alter_schema(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_schema_alter_of(stmt->ast);
	cascade_schema_rename(self->share->db, tr, &arg->name->string,
	                      &arg->name_new->string,
	                       arg->if_exists);
}

static inline void
ddl_create_partition(TableConfig* table_config, Node* node,
                     uint64_t min, uint64_t max)
{
	// create partition config
	auto config = part_config_allocate();
	auto psn = config_psn_next();
	part_config_set_id(config, psn);
	part_config_set_node(config, &node->id);
	part_config_set_range(config, min, max);
	table_config_add_partition(table_config, config);
}

static void
ddl_create_table(Session* self, Tr* tr)
{
	auto stmt   = compiler_stmt(&self->compiler);
	auto arg    = ast_table_create_of(stmt->ast);
	auto config = arg->config;
	auto db     = self->share->db;

	// ensure schema exists and not system
	auto schema = schema_mgr_find(&db->schema_mgr, &config->schema, true);
	if (! schema->config->create)
		error("system schema <%.*s> cannot be used to create objects",
		      str_size(&schema->config->name),
		      str_of(&schema->config->name));

	// todo: ensure view with the same name does not exists

	// create table partitions
	auto cluster = self->share->cluster;
	if (! cluster->list_count)
		error("system has no nodes");

	if (config->shared)
	{
		// shared table require only one partition
		auto compute = container_of(list_first(&cluster->list), Compute, link);
		ddl_create_partition(config, compute->node, 0, PARTITION_MAX);
	} else
	{
		// create partition for each node

		// partition_max / nodes_count
		int range_max      = PARTITION_MAX;
		int range_interval = range_max / cluster->list_count;
		int range_start    = 0;

		list_foreach(&cluster->list)
		{
			auto compute = list_at(Compute, link);

			// set partition range
			int range_step;
			if (list_is_last(&cluster->list, &compute->link))
				range_step = range_max - range_start;
			else
				range_step = range_interval;
			if ((range_start + range_step) > range_max)
				range_step = range_max - range_start;

			ddl_create_partition(config, compute->node,
			                     range_start,
			                     range_start + range_step);

			range_start += range_step;
		}
	}

	// create table
	table_mgr_create(&db->table_mgr, tr, config, arg->if_not_exists);
}

static void
ddl_drop_table(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_drop_of(stmt->ast);
	table_mgr_drop(&self->share->db->table_mgr, tr, &arg->schema, &arg->name,
	               arg->if_exists);
}

static void
ddl_truncate(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_truncate_of(stmt->ast);

	// truncate table
	table_mgr_truncate(&self->share->db->table_mgr, tr, &arg->schema, &arg->name,
	                   arg->if_exists);
}

static void
ddl_alter_table_set_identity(Session* self)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_alter_of(stmt->ast);

	// SET SERIAL
	auto table = table_mgr_find(&self->share->db->table_mgr, &arg->schema,
	                            &arg->name,
	                            !arg->if_exists);
	if (! table)
		return;
	sequence_set(&table->seq, arg->identity->integer);
}

static void
ddl_alter_table_set_unlogged(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_alter_of(stmt->ast);

	// truncate table
	table_mgr_set_unlogged(&self->share->db->table_mgr, tr, &arg->schema, &arg->name,
	                       arg->unlogged, arg->if_exists);
}

static void
ddl_alter_table_rename(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_alter_of(stmt->ast);
	auto db   = self->share->db;

	// RENAME TO

	// ensure schema exists
	auto schema = schema_mgr_find(&db->schema_mgr, &arg->schema_new, true);
	if (! schema->config->create)
		error("system schema <%.*s> cannot be used to create objects",
		      str_size(&schema->config->name),
		      str_of(&schema->config->name));

	// todo: ensure view with the same name does not exists

	// rename table
	table_mgr_rename(&db->table_mgr, tr, &arg->schema, &arg->name,
	                 &arg->schema_new, &arg->name_new,
	                  arg->if_exists);
}

static void
ddl_alter_table_column_rename(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_alter_of(stmt->ast);
	auto db   = self->share->db;

	// RENAME COLUMN TO

	// rename table
	table_mgr_column_rename(&db->table_mgr, tr, &arg->schema, &arg->name,
	                        &arg->column_name, &arg->name_new,
	                         arg->if_exists);
}

static void
ddl_alter_table_column_add(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_alter_of(stmt->ast);

	// COLUMN ADD name type [constraint]
	auto table_mgr = &self->share->db->table_mgr;
	auto table = table_mgr_find(table_mgr, &arg->schema, &arg->name,
	                            !arg->if_exists);
	if (! table)
		return;

	auto table_new = table_mgr_column_add(table_mgr, tr, &arg->schema, &arg->name,
	                                      arg->column, false);
	if (! table_new)
		return;

	// rebuild new table with new column in parallel per node
	Build build;
	build_init(&build, BUILD_COLUMN_ADD, self->share->cluster,
	            table,
	            table_new, arg->column, NULL);
	defer(build_free, &build);
	build_run(&build);
}

static void
ddl_alter_table_column_drop(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_alter_of(stmt->ast);

	// COLUMN DROP name
	auto table_mgr = &self->share->db->table_mgr;
	auto table = table_mgr_find(table_mgr, &arg->schema, &arg->name,
	                            !arg->if_exists);
	if (! table)
		return;

	auto table_new = table_mgr_column_drop(table_mgr, tr, &arg->schema, &arg->name,
	                                       &arg->column_name, false);
	if (! table_new)
		return;

	auto column = columns_find(&table->config->columns, &arg->column_name);
	assert(column);

	// rebuild new table with new column in parallel per node
	Build build;
	build_init(&build, BUILD_COLUMN_DROP, self->share->cluster,
	            table,
	            table_new, column, NULL);
	defer(build_free, &build);
	build_run(&build);
}

static void
ddl_alter_table(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_alter_of(stmt->ast);
	auto db   = self->share->db;
	switch (arg->type) {
	case TABLE_ALTER_RENAME:
		ddl_alter_table_rename(self, tr);
		break;
	case TABLE_ALTER_SET_IDENTITY:
		ddl_alter_table_set_identity(self);
		break;
	case TABLE_ALTER_SET_UNLOGGED:
		ddl_alter_table_set_unlogged(self, tr);
		break;
	case TABLE_ALTER_COLUMN_RENAME:
		ddl_alter_table_column_rename(self, tr);
		break;
	case TABLE_ALTER_COLUMN_ADD:
		ddl_alter_table_column_add(self, tr);
		break;
	case TABLE_ALTER_COLUMN_DROP:
		ddl_alter_table_column_drop(self, tr);
		break;
	case TABLE_ALTER_COLUMN_SET_DEFAULT:
	case TABLE_ALTER_COLUMN_UNSET_DEFAULT:
		table_mgr_column_set_default(&db->table_mgr, tr,
		                             &arg->schema,
		                             &arg->name,
		                             &arg->column_name,
		                             &arg->value,
		                              arg->if_exists);
		break;
	case TABLE_ALTER_COLUMN_SET_IDENTITY:
	case TABLE_ALTER_COLUMN_UNSET_IDENTITY:
		table_mgr_column_set_identity(&db->table_mgr, tr,
		                              &arg->schema,
		                              &arg->name,
		                              &arg->column_name,
		                              &arg->value,
		                               arg->if_exists);
		break;
	case TABLE_ALTER_COLUMN_SET_STORED:
	case TABLE_ALTER_COLUMN_UNSET_STORED:
		table_mgr_column_set_stored(&db->table_mgr, tr,
		                            &arg->schema,
		                            &arg->name,
		                            &arg->column_name,
		                            &arg->value,
		                             arg->if_exists);
		break;
	case TABLE_ALTER_COLUMN_SET_RESOLVED:
	case TABLE_ALTER_COLUMN_UNSET_RESOLVED:
		table_mgr_column_set_resolved(&db->table_mgr, tr,
		                              &arg->schema,
		                              &arg->name,
		                              &arg->column_name,
		                              &arg->value,
		                               arg->if_exists);
		break;
	}
}

static void
ddl_create_index(Session* self, Tr* tr)
{
	auto stmt    = compiler_stmt(&self->compiler);
	auto arg     = ast_index_create_of(stmt->ast);
	auto db      = self->share->db;
	auto cluster = self->share->cluster;

	// find table
	auto table = table_mgr_find(&db->table_mgr, &arg->table_schema,
	                            &arg->table_name,
	                            true);
	auto created = table_index_create(table, tr, arg->config, arg->if_not_exists);
	if (! created)
		return;

	auto index = table_find_index(table, &arg->config->name, true);

	// do parallel indexation per node
	Build build;
	build_init(&build, BUILD_INDEX, cluster, table, NULL, NULL, index);
	defer(build_free, &build);
	build_run(&build);
}

static void
ddl_drop_index(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_index_drop_of(stmt->ast);
	auto db   = self->share->db;

	// find table
	auto table = table_mgr_find(&db->table_mgr, &arg->table_schema,
	                            &arg->table_name,
	                            true);

	table_index_drop(table, tr, &arg->name, arg->if_exists);
}

static void
ddl_alter_index(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_index_alter_of(stmt->ast);
	auto db   = self->share->db;

	// find table
	auto table = table_mgr_find(&db->table_mgr, &arg->table_schema,
	                            &arg->table_name,
	                            true);

	table_index_rename(table, tr, &arg->name, &arg->name_new, arg->if_exists);
}

static void
ddl_create_node(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_node_create_of(stmt->ast);
	auto db   = self->share->db;

	auto config = node_config_allocate();
	defer(node_config_free, config);

	// set or generate node name
	if (arg->id)
	{
		node_config_set_id(config, &arg->id->string);
	} else
	{
		Uuid id;
		uuid_generate(&id, global()->random);

		char uuid[UUID_SZ];
		uuid_get(&id, uuid, sizeof(uuid));

		Str uuid_str;
		str_set_cstr(&uuid_str, uuid);
		node_config_set_id(config, &uuid_str);
	}

	// create node
	node_mgr_create(&db->node_mgr, tr, config, arg->if_not_exists);
}

static void
ddl_drop_node(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_node_drop_of(stmt->ast);
	auto db   = self->share->db;

	// drop node
	node_mgr_drop(&db->node_mgr, tr, &arg->id->string, arg->if_exists);
}

static inline void
session_execute_ddl_stmt(Session* self, Tr* tr)
{
	auto stmt = compiler_stmt(&self->compiler);
	switch (stmt->id) {
	case STMT_CREATE_SCHEMA:
		ddl_create_schema(self, tr);
		break;
	case STMT_DROP_SCHEMA:
		ddl_drop_schema(self, tr);
		break;
	case STMT_ALTER_SCHEMA:
		ddl_alter_schema(self, tr);
		break;
	case STMT_CREATE_TABLE:
		ddl_create_table(self, tr);
		break;
	case STMT_DROP_TABLE:
		ddl_drop_table(self, tr);
		break;
	case STMT_ALTER_TABLE:
		ddl_alter_table(self, tr);
		break;
	case STMT_CREATE_INDEX:
		ddl_create_index(self, tr);
		break;
	case STMT_DROP_INDEX:
		ddl_drop_index(self, tr);
		break;
	case STMT_ALTER_INDEX:
		ddl_alter_index(self, tr);
		break;
	case STMT_CREATE_NODE:
		ddl_create_node(self, tr);
		break;
	case STMT_DROP_NODE:
		ddl_drop_node(self, tr);
		break;
	case STMT_TRUNCATE:
		ddl_truncate(self, tr);
		break;
	default:
		assert(0);
		break;
	}

	// wal write
	if (tr_read_only(tr))
		return;

	WalBatch batch;
	wal_batch_init(&batch);
	defer(wal_batch_free, &batch);
	wal_batch_begin(&batch, WAL_UTILITY);
	wal_batch_add(&batch, &tr->log.log_set);
	wal_write(&self->share->db->wal, &batch);
}

void
session_execute_ddl(Session* self)
{
	// respect system read-only state
	if (var_int_of(&config()->read_only))
		error("system is in read-only mode");

	// upgrade to exclusive lock
	session_lock(self, LOCK_EXCLUSIVE);

	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	auto on_error = error_catch
	(
		// begin
		tr_begin(&tr);

		session_execute_ddl_stmt(self, &tr);

		// commit
		tr_commit(&tr);
	);
	if (unlikely(on_error))
	{
		tr_abort(&tr);
		rethrow();
	}
}
