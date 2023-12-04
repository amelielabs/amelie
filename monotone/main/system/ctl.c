
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
#include <monotone_system.h>

static Buf*
ctl_show(System* self, Stmt* stmt)
{
	auto arg  = ast_show_of(stmt->ast);
	auto name = &arg->expr->string;
	Buf* buf  = NULL;
	if (str_compare_raw(name, "users", 5))
		buf = user_mgr_list(&self->user_mgr);
	else
	if (str_compare_raw(name, "wal", 3))
		buf = wal_status(&self->db.wal);
	else
	if (str_compare_raw(name, "schemas", 7))
		buf = schema_mgr_list(&self->db.schema_mgr);
	else
	if (str_compare_raw(name, "functions", 9))
		buf = function_mgr_list(&self->function_mgr);
	else
	if (str_compare_raw(name, "tables", 6))
		buf = table_mgr_list(&self->db.table_mgr);
	else
	if (str_compare_raw(name, "partitions", 10))
		buf = table_mgr_list_partitions(&self->db.table_mgr);
	else
	if (str_compare_raw(name, "views", 5))
		buf = view_mgr_list(&self->db.view_mgr);
	else
	if (str_compare_raw(name, "all", 3))
		buf = config_list(global()->config);
	else
	{
		auto var = config_find(global()->config, name);
		if (var && var_is(var, VAR_S))
			var = NULL;
		if (unlikely(var == NULL))
			error("SHOW name: '%.*s' not found", str_size(name),
			      str_of(name));
		buf = var_msg_create(var);
	}
	return buf;
}

static void
ctl_set(Session* self, Stmt* stmt)
{
	unused(self);

	auto arg = ast_set_of(stmt->ast);
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

static void
ctl_create_user(System* self, Stmt* stmt)
{
	auto arg = ast_user_create_of(stmt->ast);
	user_mgr_create(&self->user_mgr, arg->config, arg->if_not_exists);
	hub_mgr_sync_user_cache(&self->hub_mgr, &self->user_mgr.cache);
}

static void
ctl_drop_user(System* self, Stmt* stmt)
{
	auto arg = ast_user_drop_of(stmt->ast);
	user_mgr_drop(&self->user_mgr, &arg->name->string, arg->if_exists);
	hub_mgr_sync_user_cache(&self->hub_mgr, &self->user_mgr.cache);
}

static void
ctl_alter_user(System* self, Stmt* stmt)
{
	auto arg = ast_user_alter_of(stmt->ast);
	user_mgr_alter(&self->user_mgr, arg->config);
	hub_mgr_sync_user_cache(&self->hub_mgr, &self->user_mgr.cache);
}

static void
ctl_checkpoint(System* self, Session* session, Stmt* stmt)
{
	(void)stmt;
	(void)session;

	uint64_t lsn = config_lsn();
	log("checkpoint: %" PRIu64, lsn);
	shard_mgr_checkpoint(&self->shard_mgr, lsn);
	log("checkpoint: %" PRIu64 " complete", lsn);
}

Buf*
system_ctl(System* self, Session* session, Stmt* stmt)
{
	switch (stmt->id) {
	case STMT_SHOW:
		return ctl_show(self, stmt);
	case STMT_SET:
		ctl_set(session, stmt);
		break;
	case STMT_CREATE_USER:
		ctl_create_user(self, stmt);
		break;
	case STMT_DROP_USER:
		ctl_drop_user(self, stmt);
		break;
	case STMT_ALTER_USER:
		ctl_alter_user(self, stmt);
		break;
	case STMT_CHECKPOINT:
		ctl_checkpoint(self, session, stmt);
		break;
	default:
		system_ddl(self, session, stmt);
		break;
	}
	return NULL;
}
