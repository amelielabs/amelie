
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_index.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_semantic.h>
#include <monotone_compiler.h>
#include <monotone_shard.h>
#include <monotone_hub.h>
#include <monotone_session.h>

static inline void
execute_show(Session* self, Ast* ast)
{
	// catalog lock
	session_lock(self, LOCK_SHARED);

	auto arg = ast_show_of(ast);
	Buf* buf = NULL;
	if (str_compare_raw(&arg->expr->string, "users", 5))
		rpc(global()->control->core, RPC_USER_SHOW, 1, &buf);
	else
	if (str_compare_raw(&arg->expr->string, "wal", 3))
		buf = wal_status(self->share->wal);
	else
	if (str_compare_raw(&arg->expr->string, "schemas", 7))
		buf = schema_mgr_list(&self->share->db->schema_mgr);
	else
	if (str_compare_raw(&arg->expr->string, "functions", 9))
		buf = function_mgr_list(self->share->function_mgr);
	else
	if (str_compare_raw(&arg->expr->string, "tables", 6))
		buf = table_mgr_list(&self->share->db->table_mgr);
	else
	if (str_compare_raw(&arg->expr->string, "partitions", 10))
		buf = table_mgr_list_partitions(&self->share->db->table_mgr);
	else
	if (str_compare_raw(&arg->expr->string, "views", 5))
		buf = view_mgr_list(&self->share->db->view_mgr);
	else
	if (str_compare_raw(&arg->expr->string, "all", 3))
		buf = config_list(global()->config);
	else
	{
		auto name = &arg->expr->string;
		auto var = config_find(global()->config, name);
		if (var && var_is(var, VAR_S))
			var = NULL;
		if (unlikely(var == NULL))
			error("SHOW name: '%.*s' not found", str_size(name),
				  str_of(name));
		buf = var_msg_create(var);
	}
	if (buf)
		portal_write(self->portal, buf);
}

static inline void
execute_set(Session* self, Ast* ast)
{
	unused(self);

	auto arg = ast_set_of(ast);
	auto name = &arg->name->string;

	// find variable
	auto var = config_find(global()->config, name);
	if (var && var_is(var, VAR_S))
		var = NULL;
	if (unlikely(var == NULL))
		error("SET '%.*s': variable not found", str_size(name),
		      str_of(name));
	if (unlikely(! var_is(var, VAR_R)))
		error("SET '%.*s': variable is read-only", str_size(name),
		      str_of(name));

	// set value
	auto value = arg->value;
	switch (var->type) {
	case VAR_BOOL:
	{
		if (value->id != KTRUE && value->id != KFALSE)
			error("SET '%.*s': bool value expected", str_size(name),
			      str_of(name));
		bool is_true = value->id == KTRUE;
		var_int_set(var, is_true);
		break;
	}
	case VAR_INT:
	{
		if (value->id != KINT)
			error("SET '%.*s': integer value expected", str_size(name),
			      str_of(name));
		var_int_set(var, value->integer);
		break;
	}
	case VAR_STRING:
	{
		if (value->id != KSTRING)
			error("SET '%.*s': string value expected", str_size(name),
			      str_of(name));
		var_string_set(var, &value->string);
		break;
	}
	case VAR_DATA:
	{
		error("SET '%.*s': variable cannot be changed", str_size(name),
		      str_of(name));
		break;
	}
	}

	// save state for persistent vars
	if (var_is(var, VAR_P))
		control_save_state();
}

static inline void
execute_add_storage(TableConfig* table_config, Shard* shard)
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

static inline void
execute_add_storages(Session* self, TableConfig* config)
{
	auto share = self->share;

	// reference table require only one storage
	if (config->reference)
	{
		execute_add_storage(config, NULL);
		return;
	}

	// create storage for each shard
	for (int i = 0; i < share->shard_mgr->shards_count; i++)
	{
		auto shard = share->shard_mgr->shards[i];
		execute_add_storage(config, shard);
	}
}

static inline void
execute_ddl(Session* self, Stmt* stmt)
{
	auto share = self->share;

	// get exclusive catalog lock
	session_lock(self, LOCK_EXCLUSIVE);

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
		{
			// create schema
			auto arg = ast_schema_create_of(stmt->ast);
			schema_mgr_create(share->schema_mgr, &trx, arg->config, arg->if_not_exists);
			break;
		}
		case STMT_DROP_SCHEMA:
		{
			// drop schema
			auto arg = ast_schema_drop_of(stmt->ast);
			cascade_schema_drop(share->db, &trx, &arg->name->string, arg->cascade,
			                    arg->if_exists);
			break;
		}
		case STMT_ALTER_SCHEMA:
		{
			// rename schema
			auto arg = ast_schema_alter_of(stmt->ast);
			cascade_schema_rename(share->db, &trx, &arg->name->string,
			                      &arg->name_new->string,
			                      arg->if_exists);
			break;
		}
		case STMT_CREATE_TABLE:
		{
			// create table
			auto arg = ast_table_create_of(stmt->ast);

			// ensure schema exists and not system
			auto schema = schema_mgr_find(share->schema_mgr, &arg->config->schema, true);
			if (! schema->config->create)
				error("system schema <%.*s> cannot be used to create objects",
				      str_size(&schema->config->name),
				      str_of(&schema->config->name));

			// ensure view with the same name does not exists
			auto view = view_mgr_find(share->view_mgr, &arg->config->schema,
			                          &arg->config->name, false);
			if (unlikely(view))
				error("view <%.*s> with the same name exists",
				      str_size(&arg->config->name),
				      str_of(&arg->config->name));

			// configure sharded table storages
			execute_add_storages(self, arg->config);

			table_mgr_create(share->table_mgr, &trx, arg->config, arg->if_not_exists);
			break;
		}
		case STMT_DROP_TABLE:
		{
			auto arg = ast_table_drop_of(stmt->ast);

			// drop table and related partitions
			cascade_table_drop(share->db, &trx, &arg->schema, &arg->name,
			                   arg->if_exists);
			break;
		}
		case STMT_ALTER_TABLE:
		{
			auto arg = ast_table_alter_of(stmt->ast);

			// SET SERIAL
			if (arg->serial)
			{
				auto table = table_mgr_find(share->table_mgr, &arg->schema,
				                            &arg->name,
				                            !arg->if_exists);
				if (! table)
					break;

				serial_set(&table->serial, arg->serial->integer);
				break;
			}

			// RENAME TO

			// ensure schema exists
			auto schema = schema_mgr_find(share->schema_mgr, &arg->schema_new, true);
			if (! schema->config->create)
				error("system schema <%.*s> cannot be used to create objects",
				      str_size(&schema->config->name),
				      str_of(&schema->config->name));

			// ensure view with the same name does not exists
			auto view = view_mgr_find(share->view_mgr, &arg->schema_new,
			                          &arg->name_new, false);
			if (unlikely(view))
				error("view <%.*s> with the same name exists",
				      str_size(&arg->name_new),
				      str_of(&arg->name_new));

			// rename table
			table_mgr_rename(share->table_mgr, &trx, &arg->schema, &arg->name,
			                 &arg->schema_new, &arg->name_new,
			                 arg->if_exists);
			break;
		}

		case STMT_CREATE_VIEW:
		{
			// create view
			auto arg = ast_view_create_of(stmt->ast);

			// ensure schema exists
			auto schema = schema_mgr_find(share->schema_mgr, &arg->config->schema, true);
			if (! schema->config->create)
				error("system schema <%.*s> cannot be used to create objects",
				      str_size(&schema->config->name),
				      str_of(&schema->config->name));

			// ensure table with the same name does not exists
			auto table = table_mgr_find(share->table_mgr, &arg->config->schema,
			                            &arg->config->name, false);
			if (unlikely(table))
				error("table <%.*s> with the same name exists",
				      str_size(&arg->config->name),
				      str_of(&arg->config->name));

			view_mgr_create(share->view_mgr, &trx, arg->config, arg->if_not_exists);
			break;
		}
		case STMT_DROP_VIEW:
		{
			// drop view
			auto arg = ast_view_drop_of(stmt->ast);
			view_mgr_drop(share->view_mgr, &trx, &arg->schema, &arg->name,
			              arg->if_exists);
			break;
		}
		case STMT_ALTER_VIEW:
		{
			// alter view
			auto arg = ast_view_alter_of(stmt->ast);

			// ensure schema exists
			auto schema = schema_mgr_find(share->schema_mgr, &arg->schema_new, true);
			if (! schema->config->create)
				error("system schema <%.*s> cannot be used to create objects",
				      str_size(&schema->config->name),
				      str_of(&schema->config->name));

			// ensure table with the same name does not exists
			auto table = table_mgr_find(share->table_mgr, &arg->schema_new,
			                            &arg->name_new, false);
			if (unlikely(table))
				error("table <%.*s> with the same name exists",
				      str_size(&arg->name_new),
				      str_of(&arg->name_new));

			// rename view
			view_mgr_rename(share->view_mgr, &trx, &arg->schema, &arg->name,
			                &arg->schema_new, &arg->name_new,
			                arg->if_exists);
			break;
		}
		default:
			assert(0);
			break;
		}

		// wal write
		if (! transaction_read_only(&trx))
		{
			log_set_add(&self->log_set, &trx.log);
			wal_write(share->wal, &self->log_set);
		}

		// commit
		transaction_set_lsn(&trx, trx.log.lsn);
		transaction_commit(&trx);
		transaction_free(&trx);
	}

	if (catch(&e))
	{
		transaction_abort(&trx);
		transaction_free(&trx);
		rethrow();
	}
}

#if 0
static inline void
execute_checkpoint(Session* self, Ast* ast)
{
	auto share = self->share;
	(void)ast;

	auto on_complete = condition_create();
	guard(guard, condition_free, on_complete);

	for (int i = 0; i < share->shard_mgr->shards_count; i++)
	{
		auto shard = share->shard_mgr->shards[i];

		pid_t pid = snapshot(share->storage_mgr, on_complete, &shard->config->id);
		condition_wait(on_complete, -1);

		int status = 0;
		int rc = waitpid(pid, &status, 0);
		if (rc == -1)
			error_system();

		bool failed = false;
		if (WIFEXITED(status))
		{
			if (WEXITSTATUS(status) == EXIT_FAILURE)
				failed = true;
		} else {
			failed = true;
		}

		if (failed)
			error("failed to create snapshot");
	}
}

static inline void
execute_checkpoint(Session* self, Ast* ast)
{
	auto share = self->share;
	(void)ast;

	struct
	{
		pid_t      pid;
		Condition* on_complete;
	} state[share->shard_mgr->shards_count];

	for (int i = 0; i < share->shard_mgr->shards_count; i++)
	{
		state[i].pid = -1;
		state[i].on_complete = condition_create();
	}

	session_lock(self, LOCK_EXCLUSIVE);

	// execute
	for (int i = 0; i < share->shard_mgr->shards_count; i++)
	{
		auto shard = share->shard_mgr->shards[i];
		state[i].pid = snapshot(share->storage_mgr, state[i].on_complete,
		                        &shard->config->id);
	}

	session_unlock(self);

	// wait for completion
	bool failed = false;
	for (int i = 0; i < share->shard_mgr->shards_count; i++)
	{
		condition_wait(state[i].on_complete, -1);
		condition_free(state[i].on_complete);

		int status = 0;
		int rc = waitpid(state[i].pid, &status, 0);
		if (rc == -1)
			error_system();

		if (WIFEXITED(status))
		{
			if (WEXITSTATUS(status) == EXIT_FAILURE)
				failed = true;
		} else {
			failed = true;
		}
	}

	if (failed)
		error("failed to create snapshot");
}
#endif

static inline void
execute_checkpoint(Session* self, Ast* ast)
{
	(void)self;
	(void)ast;

	control_save_catalog();
}

void
session_execute_utility(Session* self)
{
	auto stmt = compiler_first(&self->compiler);
	switch (stmt->id) {
	case STMT_SHOW:
		execute_show(self, stmt->ast);
		break;

	case STMT_SET:
		execute_set(self, stmt->ast);
		break;

	case STMT_CHECKPOINT:
		execute_checkpoint(self, stmt->ast);
		break;

	case STMT_CREATE_USER:
	{
		auto arg = ast_user_create_of(stmt->ast);
		rpc(global()->control->core, RPC_USER_CREATE, 2,
		    arg->config, arg->if_not_exists);
		break;
	}

	case STMT_DROP_USER:
	{
		auto arg = ast_user_drop_of(stmt->ast);
		rpc(global()->control->core, RPC_USER_DROP, 2,
		    &arg->name->string, arg->if_exists);
		break;
	}

	case STMT_ALTER_USER:
	{
		auto arg = ast_user_alter_of(stmt->ast);
		rpc(global()->control->core, RPC_USER_ALTER, 1, arg->config);
		break;
	}

	case STMT_CREATE_SCHEMA:
	case STMT_DROP_SCHEMA:
	case STMT_ALTER_SCHEMA:
	case STMT_CREATE_TABLE:
	case STMT_DROP_TABLE:
	case STMT_ALTER_TABLE:
	case STMT_CREATE_VIEW:
	case STMT_DROP_VIEW:
	case STMT_ALTER_VIEW:
		execute_ddl(self, stmt);
		break;

	default:
		assert(0);
		break;
	}
}
