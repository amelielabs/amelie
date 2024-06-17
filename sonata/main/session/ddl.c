
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_repl.h>
#include <sonata_cluster.h>
#include <sonata_frontend.h>
#include <sonata_session.h>

static void
ddl_create_schema(Session* self, Transaction* trx)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_schema_create_of(stmt->ast);
	schema_mgr_create(&self->share->db->schema_mgr, trx, arg->config,
	                  arg->if_not_exists);
}

static void
ddl_drop_schema(Session* self, Transaction* trx)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_schema_drop_of(stmt->ast);
	cascade_schema_drop(self->share->db, trx, &arg->name->string, arg->cascade,
	                    arg->if_exists);
}

static void
ddl_alter_schema(Session* self, Transaction* trx)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_schema_alter_of(stmt->ast);
	cascade_schema_rename(self->share->db, trx, &arg->name->string,
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
	part_config_set_node(config, &node->config->id);
	part_config_set_range(config, min, max);
	table_config_add_partition(table_config, config);
}

static void
ddl_create_table(Session* self, Transaction* trx)
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

	// ensure view with the same name does not exists
	auto view = view_mgr_find(&db->view_mgr, &config->schema,
	                          &config->name, false);
	if (unlikely(view))
		error("view <%.*s> with the same name exists",
		      str_size(&config->name),
		      str_of(&config->name));

	// create table partitions
	auto cluster = self->share->cluster;
	if (! cluster->list_count)
		error("system has no nodes");

	if (config->reference)
	{
		// reference table require only one partition
		auto node = container_of(list_first(&cluster->list), Node, link);
		ddl_create_partition(config, node, 0, PARTITION_MAX);
	} else
	{
		// create partition for each node

		// partition_max / nodes_count
		int range_max      = PARTITION_MAX;
		int range_interval = range_max / cluster->list_count;
		int range_start    = 0;

		list_foreach(&cluster->list)
		{
			auto node = list_at(Node, link);

			// set partition range
			int range_step;
			if (list_is_last(&cluster->list, &node->link))
				range_step = range_max - range_start;
			else
				range_step = range_interval;
			if ((range_start + range_step) > range_max)
				range_step = range_max - range_start;

			ddl_create_partition(config, node,
			                     range_start,
			                     range_start + range_step);

			range_start += range_step;
		}
	}

	// create table
	table_mgr_create(&db->table_mgr, trx, config, arg->if_not_exists);
}

static void
ddl_drop_table(Session* self, Transaction* trx)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_drop_of(stmt->ast);
	cascade_table_drop(self->share->db, trx, &arg->schema, &arg->name,
	                   arg->if_exists);
}

static void
ddl_alter_table_set_serial(Session* self)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_alter_of(stmt->ast);

	// SET SERIAL
	auto table = table_mgr_find(&self->share->db->table_mgr, &arg->schema,
	                            &arg->name,
	                            !arg->if_exists);
	if (! table)
		return;
	serial_set(&table->serial, arg->serial->integer);
}

static void
ddl_alter_table_rename(Session* self, Transaction* trx)
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

	// ensure view with the same name does not exists
	auto view = view_mgr_find(&db->view_mgr, &arg->schema_new,
	                          &arg->name_new, false);
	if (unlikely(view))
		error("view <%.*s> with the same name exists",
		      str_size(&arg->name_new),
		      str_of(&arg->name_new));

	// rename table
	table_mgr_rename(&db->table_mgr, trx, &arg->schema, &arg->name,
	                 &arg->schema_new, &arg->name_new,
	                  arg->if_exists);
}

static void
ddl_alter_table(Session* self, Transaction* trx)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_table_alter_of(stmt->ast);
	if (arg->serial)
	{
		ddl_alter_table_set_serial(self);
		return;
	}
	ddl_alter_table_rename(self, trx);
}

static void
ddl_create_index(Session* self, Transaction* trx)
{
	auto stmt    = compiler_stmt(&self->compiler);
	auto arg     = ast_index_create_of(stmt->ast);
	auto db      = self->share->db;
	auto cluster = self->share->cluster;

	// find table
	auto table = table_mgr_find(&db->table_mgr, &arg->table_schema,
	                            &arg->table_name,
	                            true);
	auto created = table_index_create(table, trx, arg->config, arg->if_not_exists);
	if (! created)
		return;

	auto index = table_find_index(table, &arg->config->name, true);

	// do parallel indexation per node
	Indexate indexate;
	indexate_init(&indexate, cluster, table, index);
	guard(indexate_free, &indexate);
	indexate_run(&indexate);
}

static void
ddl_drop_index(Session* self, Transaction* trx)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_index_drop_of(stmt->ast);
	auto db   = self->share->db;

	// find table
	auto table = table_mgr_find(&db->table_mgr, &arg->table_schema,
	                            &arg->table_name,
	                            true);

	table_index_drop(table, trx, &arg->name, arg->if_exists);
}

static void
ddl_alter_index(Session* self, Transaction* trx)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_index_alter_of(stmt->ast);
	auto db   = self->share->db;

	// find table
	auto table = table_mgr_find(&db->table_mgr, &arg->table_schema,
	                            &arg->table_name,
	                            true);

	table_index_rename(table, trx, &arg->name, &arg->name_new, arg->if_exists);
}

static void
ddl_create_view(Session* self, Transaction* trx)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_view_create_of(stmt->ast);
	auto db   = self->share->db;

	// ensure schema exists
	auto schema = schema_mgr_find(&db->schema_mgr, &arg->config->schema, true);
	if (! schema->config->create)
		error("system schema <%.*s> cannot be used to create objects",
		      str_size(&schema->config->name),
		      str_of(&schema->config->name));

	// ensure table with the same name does not exists
	auto table = table_mgr_find(&db->table_mgr, &arg->config->schema,
	                            &arg->config->name, false);
	if (unlikely(table))
		error("table <%.*s> with the same name exists",
		      str_size(&arg->config->name),
		      str_of(&arg->config->name));

	// create view
	view_mgr_create(&db->view_mgr, trx, arg->config,
	                 arg->if_not_exists);
}

static void
ddl_drop_view(Session* self, Transaction* trx)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_view_drop_of(stmt->ast);
	view_mgr_drop(&self->share->db->view_mgr, trx, &arg->schema, &arg->name,
	              arg->if_exists);
}

static void
ddl_alter_view(Session* self, Transaction* trx)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_view_alter_of(stmt->ast);
	auto db   = self->share->db;

	// ensure schema exists
	auto schema = schema_mgr_find(&db->schema_mgr, &arg->schema_new, true);
	if (! schema->config->create)
		error("system schema <%.*s> cannot be used to create objects",
		      str_size(&schema->config->name),
		      str_of(&schema->config->name));

	// ensure table with the same name does not exists
	auto table = table_mgr_find(&db->table_mgr, &arg->schema_new,
	                            &arg->name_new, false);
	if (unlikely(table))
		error("table <%.*s> with the same name exists",
		      str_size(&arg->name_new),
		      str_of(&arg->name_new));

	// rename view
	view_mgr_rename(&db->view_mgr, trx, &arg->schema, &arg->name,
	                &arg->schema_new, &arg->name_new,
	                 arg->if_exists);
}

void
session_execute_ddl(Session* self)
{
	// respect system read-only state
	if (var_int_of(&config()->read_only))
		error("system is in read-only mode");

	// upgrade to exclusive lock
	session_lock(self, SESSION_LOCK_EXCLUSIVE);

	Transaction trx;
	transaction_init(&trx);

	WalBatch wal_batch;
	wal_batch_init(&wal_batch);

	Exception e;
	if (enter(&e))
	{
		// begin
		transaction_begin(&trx);

		auto stmt = compiler_stmt(&self->compiler);
		switch (stmt->id) {
		case STMT_CREATE_SCHEMA:
			ddl_create_schema(self, &trx);
			break;
		case STMT_DROP_SCHEMA:
			ddl_drop_schema(self, &trx);
			break;
		case STMT_ALTER_SCHEMA:
			ddl_alter_schema(self, &trx);
			break;
		case STMT_CREATE_TABLE:
			ddl_create_table(self, &trx);
			break;
		case STMT_DROP_TABLE:
			ddl_drop_table(self, &trx);
			break;
		case STMT_ALTER_TABLE:
			ddl_alter_table(self, &trx);
			break;
		case STMT_CREATE_INDEX:
			ddl_create_index(self, &trx);
			break;
		case STMT_DROP_INDEX:
			ddl_drop_index(self, &trx);
			break;
		case STMT_ALTER_INDEX:
			ddl_alter_index(self, &trx);
			break;
		case STMT_CREATE_VIEW:
			ddl_create_view(self, &trx);
			break;
		case STMT_DROP_VIEW:
			ddl_drop_view(self, &trx);
			break;
		case STMT_ALTER_VIEW:
			ddl_alter_view(self, &trx);
			break;
		default:
			assert(0);
			break;
		}

		// wal write
		if (! transaction_read_only(&trx))
		{
			wal_batch_begin(&wal_batch, WAL_UTILITY);
			wal_batch_add(&wal_batch, &trx.log.log_set);
			wal_write(&self->share->db->wal, &wal_batch);
		}

		// commit
		transaction_commit(&trx);
		transaction_free(&trx);
		wal_batch_free(&wal_batch);
	}

	if (leave(&e))
	{
		transaction_abort(&trx);
		transaction_free(&trx);

		wal_batch_free(&wal_batch);
		rethrow();
	}
}
