
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
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
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
#include <sonata_shard.h>
#include <sonata_frontend.h>
#include <sonata_session.h>
#include <sonata_main.h>

static void
ddl_create_schema(System* self, Transaction* trx, Stmt* stmt)
{
	auto arg = ast_schema_create_of(stmt->ast);
	schema_mgr_create(&self->db.schema_mgr, trx, arg->config, arg->if_not_exists);
}

static void
ddl_drop_schema(System* self, Transaction* trx, Stmt* stmt)
{
	auto arg = ast_schema_drop_of(stmt->ast);
	cascade_schema_drop(&self->db, trx, &arg->name->string, arg->cascade,
	                    arg->if_exists);
}

static void
ddl_alter_schema(System* self, Transaction* trx, Stmt* stmt)
{
	auto arg = ast_schema_alter_of(stmt->ast);
	cascade_schema_rename(&self->db, trx, &arg->name->string,
	                      &arg->name_new->string,
	                       arg->if_exists);
}

static inline void
ddl_create_partition(TableConfig* table_config, Shard* shard)
{
	// create partition config
	auto config = part_config_allocate();
	auto ssn = config_ssn_next();
	part_config_set_id(config, ssn);
	part_config_set_shard(config, &shard->config->id);
	part_config_set_range(config, shard->config->min, shard->config->max);
	table_config_add_partition(table_config, config);
}

static void
ddl_create_table(System* self, Transaction* trx, Stmt* stmt)
{
	auto arg    = ast_table_create_of(stmt->ast);
	auto config = arg->config;
	auto db     = &self->db;

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

	// reference table require only one partition
	auto shard_mgr = &self->shard_mgr;
	if (config->reference)
	{
		auto shard = shard_mgr->shards[0];
		ddl_create_partition(config, shard);
	} else
	{
		// create partition config for each shard
		for (int i = 0; i < shard_mgr->shards_count; i++)
		{
			auto shard = shard_mgr->shards[i];
			ddl_create_partition(config, shard);
		}
	}

	// create table
	table_mgr_create(&db->table_mgr, trx, config, arg->if_not_exists);
}

static void
ddl_drop_table(System* self, Transaction* trx, Stmt* stmt)
{
	auto arg = ast_table_drop_of(stmt->ast);
	cascade_table_drop(&self->db, trx, &arg->schema, &arg->name,
	                    arg->if_exists);
}

static void
ddl_alter_table_set_serial(System* self, Stmt* stmt)
{
	auto arg = ast_table_alter_of(stmt->ast);

	// SET SERIAL
	auto table = table_mgr_find(&self->db.table_mgr, &arg->schema,
	                            &arg->name,
	                            !arg->if_exists);
	if (! table)
		return;

	serial_set(&table->serial, arg->serial->integer);
}

static void
ddl_alter_table_rename(System* self, Transaction* trx, Stmt* stmt)
{
	auto arg = ast_table_alter_of(stmt->ast);

	// RENAME TO

	// ensure schema exists
	auto schema = schema_mgr_find(&self->db.schema_mgr, &arg->schema_new, true);
	if (! schema->config->create)
		error("system schema <%.*s> cannot be used to create objects",
		      str_size(&schema->config->name),
		      str_of(&schema->config->name));

	// ensure view with the same name does not exists
	auto view = view_mgr_find(&self->db.view_mgr, &arg->schema_new,
	                          &arg->name_new, false);
	if (unlikely(view))
		error("view <%.*s> with the same name exists",
		      str_size(&arg->name_new),
		      str_of(&arg->name_new));

	// rename table
	table_mgr_rename(&self->db.table_mgr, trx, &arg->schema, &arg->name,
	                 &arg->schema_new, &arg->name_new,
	                  arg->if_exists);
}

static void
ddl_alter_table(System* self, Transaction* trx, Stmt* stmt)
{
	auto arg = ast_table_alter_of(stmt->ast);
	if (arg->serial)
	{
		ddl_alter_table_set_serial(self, stmt);
		return;
	}
	ddl_alter_table_rename(self, trx, stmt);
}

static void
ddl_create_view(System* self, Transaction* trx, Stmt* stmt)
{
	auto arg = ast_view_create_of(stmt->ast);

	// ensure schema exists
	auto schema = schema_mgr_find(&self->db.schema_mgr, &arg->config->schema, true);
	if (! schema->config->create)
		error("system schema <%.*s> cannot be used to create objects",
		      str_size(&schema->config->name),
		      str_of(&schema->config->name));

	// ensure table with the same name does not exists
	auto table = table_mgr_find(&self->db.table_mgr, &arg->config->schema,
	                            &arg->config->name, false);
	if (unlikely(table))
		error("table <%.*s> with the same name exists",
		      str_size(&arg->config->name),
		      str_of(&arg->config->name));

	// create view
	view_mgr_create(&self->db.view_mgr, trx, arg->config,
	                 arg->if_not_exists);
}

static void
ddl_drop_view(System* self, Transaction* trx, Stmt* stmt)
{
	auto arg = ast_view_drop_of(stmt->ast);
	view_mgr_drop(&self->db.view_mgr, trx, &arg->schema, &arg->name,
	              arg->if_exists);
}

static void
ddl_alter_view(System* self, Transaction* trx, Stmt* stmt)
{
	auto arg = ast_view_alter_of(stmt->ast);

	// ensure schema exists
	auto schema = schema_mgr_find(&self->db.schema_mgr, &arg->schema_new, true);
	if (! schema->config->create)
		error("system schema <%.*s> cannot be used to create objects",
		      str_size(&schema->config->name),
		      str_of(&schema->config->name));

	// ensure table with the same name does not exists
	auto table = table_mgr_find(&self->db.table_mgr, &arg->schema_new,
	                            &arg->name_new, false);
	if (unlikely(table))
		error("table <%.*s> with the same name exists",
		      str_size(&arg->name_new),
		      str_of(&arg->name_new));

	// rename view
	view_mgr_rename(&self->db.view_mgr, trx, &arg->schema, &arg->name,
	                &arg->schema_new, &arg->name_new,
	                 arg->if_exists);
}

void
system_ddl(System* self, Session* session, Stmt* stmt)
{
	(void)session;

	// get exclusive session lock
	frontend_mgr_lock(&self->frontend_mgr);

	Transaction trx;
	transaction_init(&trx);

	WalBatch wal_batch;
	wal_batch_init(&wal_batch);

	Exception e;
	if (enter(&e))
	{
		// begin
		transaction_begin(&trx);

		switch (stmt->id) {
		case STMT_CREATE_SCHEMA:
			ddl_create_schema(self, &trx, stmt);
			break;
		case STMT_DROP_SCHEMA:
			ddl_drop_schema(self, &trx, stmt);
			break;
		case STMT_ALTER_SCHEMA:
			ddl_alter_schema(self, &trx, stmt);
			break;
		case STMT_CREATE_TABLE:
			ddl_create_table(self, &trx, stmt);
			break;
		case STMT_DROP_TABLE:
			ddl_drop_table(self, &trx, stmt);
			break;
		case STMT_ALTER_TABLE:
			ddl_alter_table(self, &trx, stmt);
			break;
		case STMT_CREATE_VIEW:
			ddl_create_view(self, &trx, stmt);
			break;
		case STMT_DROP_VIEW:
			ddl_drop_view(self, &trx, stmt);
			break;
		case STMT_ALTER_VIEW:
			ddl_alter_view(self, &trx, stmt);
			break;
		default:
			assert(0);
			break;
		}

		// wal write
		if (! transaction_read_only(&trx))
		{
			uint64_t lsn = config_lsn() + 1;
			wal_batch_begin(&wal_batch, lsn);
			wal_batch_add(&wal_batch, &trx.log.log_set);
			wal_write(&self->db.wal, &wal_batch);
			config_lsn_set(lsn);
		}

		// commit
		transaction_commit(&trx);
		transaction_free(&trx);
		wal_batch_free(&wal_batch);
	}

	// unlock sessions
	frontend_mgr_unlock(&self->frontend_mgr);

	if (leave(&e))
	{
		// rpc by unlock changes code
		so_self()->error.code = ERROR;

		transaction_abort(&trx);
		transaction_free(&trx);

		wal_batch_free(&wal_batch);
		rethrow();
	}
}
