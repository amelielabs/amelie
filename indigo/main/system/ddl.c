
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_client.h>
#include <indigo_server.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>
#include <indigo_semantic.h>
#include <indigo_compiler.h>
#include <indigo_shard.h>
#include <indigo_hub.h>
#include <indigo_session.h>
#include <indigo_system.h>

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
ddl_create_storage(TableConfig* table_config, Shard* shard)
{
	// create storage config
	auto config = storage_config_allocate();
	Uuid id;
	uuid_mgr_generate(global()->uuid_mgr, &id);
	storage_config_set_id(config, &id);
	if (shard)
	{
		storage_config_set_shard(config, &shard->config->id);
		storage_config_set_range(config, shard->config->range_start,
		                         shard->config->range_end);
	}
	table_config_add_storage(table_config, config);
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

	// reference table require only one storage
	if (config->reference)
	{
		ddl_create_storage(config, NULL);
	} else
	{
		// create storage config for each shard
		auto shard_mgr = &self->shard_mgr;
		for (int i = 0; i < shard_mgr->shards_count; i++)
		{
			auto shard = shard_mgr->shards[i];
			ddl_create_storage(config, shard);
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
	// get exclusive session lock
	hub_mgr_session_lock(&self->hub_mgr);

	Transaction trx;
	transaction_init(&trx);

	Exception e;
	if (try(&e))
	{
		// begin
		transaction_begin(&trx);
		transaction_set_auto_commit(&trx);

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
			log_set_add(&session->log_set, &trx.log);
			wal_write(&self->db.wal, &session->log_set);
		}

		// commit
		transaction_set_lsn(&trx, trx.log.lsn);
		transaction_commit(&trx);
		transaction_free(&trx);
	}

	// unlock sessions
	hub_mgr_session_unlock(&self->hub_mgr);

	if (catch(&e))
	{
		// rpc by unlock changes code
		mn_self()->error.code = ERROR;

		transaction_abort(&trx);
		transaction_free(&trx);
		rethrow();
	}
}
