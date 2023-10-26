
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
#include <monotone_key.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>
#include <monotone_shard.h>
#include <monotone_session.h>

static inline void
execute_create_storages(Session* self, Table* table)
{
	auto share = self->share;

	// create storage for each shard
	for (int i = 0; i < share->shard_mgr->shards_count; i++)
	{
		auto shard = share->shard_mgr->shards[i];

		// create storage config
		auto config = storage_config_allocate();
		guard(config_guard, storage_config_free, config);

		Uuid id;
		uuid_mgr_generate(global()->uuid_mgr, &id);
		storage_config_set_id(config, &id);
		storage_config_set_id_table(config, &table->config->id);
		storage_config_set_id_shard(config, &shard->config->id);
		storage_config_set_range(config, shard->config->range_start,
		                         shard->config->range_end);

		// create storage
		auto storage = storage_mgr_create(share->storage_mgr, config);
		unguard(&config_guard);

		// create primary index per storage

		// create index config
		auto index_config = index_config_allocate();
		guard(index_config_guard, index_config_free, index_config);
		Str name;
		str_set_cstr(&name, "primary");
		index_config_set_name(index_config, &name);
		index_config_set_type(index_config, INDEX_TREE);
		index_config_set_primary(index_config, true);
		key_copy(&index_config->key, &table->config->key);

		// create tree index
		auto index = tree_allocate(index_config, &storage->config->id);
		unguard(&index_config_guard);
		index_config_free(index_config);

		// attach index to the storage
		storage_mgr_attach(share->storage_mgr, storage, index);

		// attach and start storage on shard
		rpc(&shard->task.channel, RPC_STORAGE_ATTACH, 1, storage);
	}
}

static inline void
execute_create_table(Session* self, Ast* ast)
{
	auto trx = &self->trx;
	auto arg = ast_table_create_of(ast);
	auto share = self->share;

	// get exclusive catalog lock
	session_lock(self, LOCK_EXCLUSIVE);

	transaction_begin(trx);

	// create table
	table_mgr_create(share->table_mgr, trx, arg->config, arg->if_not_exists);
	if (transaction_read_only(trx))
	{
		// table exists
		transaction_commit(trx);
		return;
	}

	// wal write
	log_set_add(&self->log_set, &trx->log);
	wal_write(share->wal, &self->log_set);

	transaction_set_lsn(trx, trx->log.lsn);
	transaction_commit(trx);

	// create storage for each shard
	auto table = table_mgr_find(share->table_mgr, &arg->config->name, true);
	execute_create_storages(self, table);
}

static inline void
execute_drop_table(Session* self, Ast* ast)
{
	auto trx = &self->trx;
	auto arg = ast_table_drop_of(ast);
	auto share = self->share;

	// get exclusive catalog lock
	session_lock(self, LOCK_EXCLUSIVE);

	transaction_begin(trx);

	// drop table
	table_mgr_drop(share->table_mgr, trx, &arg->name->string, arg->if_exists);
	if (transaction_read_only(trx))
	{
		// table does not exists
		transaction_commit(trx);
		return;
	}

	// wal write
	log_set_add(&self->log_set, &trx->log);
	wal_write(share->wal, &self->log_set);

	transaction_set_lsn(trx, trx->log.lsn);
	transaction_commit(trx);
}

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
	if (str_compare_raw(&arg->expr->string, "tables", 6))
		buf = table_mgr_list(&self->share->db->table_mgr);
	else
	if (str_compare_raw(&arg->expr->string, "views", 5))
		buf = meta_mgr_list(&self->share->db->meta_mgr);
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
		control_save_config();
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

	case STMT_CREATE_TABLE:
		execute_create_table(self, stmt->ast);
		break;

	case STMT_DROP_TABLE:
		execute_drop_table(self, stmt->ast);
		break;

	case STMT_ALTER_USER:
		// todo
		break;

	case STMT_ALTER_TABLE:
		// todo
		break;

	default:
		assert(0);
		break;
	}
}
