
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
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_host.h>
#include <amelie_compute.h>
#include <amelie_session.h>

static void
ctl_show(Session* self)
{
	auto stmt  = compiler_stmt(&self->compiler);
	auto arg   = ast_show_of(stmt->ast);
	auto share = self->share;
	Buf* buf   = NULL;

	switch (arg->type) {
	case SHOW_USERS:
		buf = user_mgr_list(share->user_mgr, NULL);
		break;
	case SHOW_USER:
		buf = user_mgr_list(share->user_mgr, &arg->name);
		break;
	case SHOW_REPLICAS:
		buf = replica_mgr_list(&share->repl->replica_mgr, NULL);
		break;
	case SHOW_REPLICA:
	{
		Uuid id;
		uuid_set(&id, &arg->name);
		buf = replica_mgr_list(&share->repl->replica_mgr, &id);
		break;
	}
	case SHOW_COMPUTE:
		buf = node_mgr_list(&share->db->node_mgr, NULL);
		break;
	case SHOW_REPL:
		buf = repl_status(share->repl);
		break;
	case SHOW_WAL:
		buf = wal_status(&share->db->wal);
		break;
	case SHOW_STATUS:
		rpc(global()->control->system, RPC_SHOW_STATUS, 1, &buf);
		break;
	case SHOW_SCHEMAS:
		buf = schema_mgr_list(&share->db->schema_mgr, NULL, arg->extended);
		break;
	case SHOW_SCHEMA:
		buf = schema_mgr_list(&share->db->schema_mgr, &arg->name, arg->extended);
		break;
	case SHOW_TABLES:
	{
		Str* schema = NULL;
		if (! str_empty(&arg->schema))
			schema = &arg->schema;
		buf = table_mgr_list(&share->db->table_mgr, schema, NULL, arg->extended);
		break;
	}
	case SHOW_TABLE:
		buf = table_mgr_list(&share->db->table_mgr, &arg->schema,
		                     &arg->name, arg->extended);
		break;
	case SHOW_CONFIG_ALL:
		buf = vars_list(&global()->config->vars, &self->local.config.vars);
		break;
	case SHOW_CONFIG:
	{
		// find local variable first
		auto var = vars_find(&self->local.config.vars, &arg->name);
		if (! var)
			var = vars_find(&global()->config->vars, &arg->name);
		if (var && var_is(var, VAR_S))
			var = NULL;
		if (unlikely(var == NULL))
			stmt_error(stmt, arg->name_ast, "variable not found");
		buf = buf_create();
		var_encode(var, buf);
		break;
	}
	}
	defer_buf(buf);

	// write content
	auto format = self->local.format;
	if (! str_empty(&arg->format))
		format = &arg->format;
	content_write_json(&self->content, format, &arg->section, buf);
}

static void
ctl_set(Session* self)
{
	auto stmt  = compiler_stmt(&self->compiler);
	auto arg   = ast_set_of(stmt->ast);
	auto name  = &arg->name->string;
	auto local = &self->local;

	// find local variable first
	auto var_global = false;
	auto var = vars_find(&local->config.vars, name);
	if (! var)
	{
		var = vars_find(&global()->config->vars, name);
		var_global = true;
	}
	if (var && var_is(var, VAR_S))
		var = NULL;

	if (unlikely(var == NULL))
		stmt_error(stmt, arg->name, "variable not found");

	// ensure the variable can be changed online
	if (unlikely(! var_is(var, VAR_C)))
		stmt_error(stmt, arg->name, "variable cannot be changed");

	// allow to change only local variables
	if (var_global)
		stmt_error(stmt, arg->name, "variable cannot be changed online");

	// validate and set timezone first
	auto value = arg->value;
	if (var == &local->config.timezone)
	{
		if (value->id != KSTRING)
			stmt_error(stmt, value, "string expected");
		auto timezone = timezone_mgr_find(global()->timezone_mgr, &value->string);
		if (! timezone)
			stmt_error(stmt, value, "unable to find timezone");

		// set new timezone
		local->timezone = timezone;
	}

	// set value
	switch (var->type) {
	case VAR_BOOL:
	{
		if (value->id != KTRUE && value->id != KFALSE)
			stmt_error(stmt, value, "bool expected");
		bool is_true = value->id == KTRUE;
		var_int_set(var, is_true);
		break;
	}
	case VAR_INT:
	{
		if (value->id != KINT)
			stmt_error(stmt, value, "integer expected");
		var_int_set(var, value->integer);
		break;
	}
	case VAR_STRING:
	{
		if (value->id != KSTRING)
			stmt_error(stmt, value, "string expected");
		var_string_set(var, &value->string);
		break;
	}
	case VAR_JSON:
	{
		stmt_error(stmt, arg->name, "variable cannot be changed");
		break;
	}
	}
}

static void
ctl_token(Session* self)
{
	auto user_mgr = self->share->user_mgr;
	auto stmt = compiler_stmt(&self->compiler);
	auto arg = ast_token_create_of(stmt->ast);

	// find user
	auto user = user_cache_find(&user_mgr->cache, &arg->user->string);
	if (! user)
		stmt_error(stmt, arg->user, "user not found");

	// ensure user has a secret
	if (str_empty(&user->config->secret))
		stmt_error(stmt, arg->user, "user has no secret");

	// set expire timestamp
	Timestamp expire;
	timestamp_init(&expire);
	timestamp_set_unixtime(&expire, time_us());

	Interval iv;
	interval_init(&iv);
	if (arg->expire) {
		interval_set(&iv, &arg->expire->string);
	} else
	{
		Str str;
		str_set_cstr(&str, "1 year");
		interval_set(&iv, &str);
	}
	timestamp_add(&expire, &iv);

	// generate token
	auto jwt = jwt_create(&user->config->name, &user->config->secret, &expire);
	defer_buf(jwt);
	auto buf = buf_create();
	encode_buf(buf, jwt);

	// write content
	Str name;
	str_set(&name, "token", 5);
	content_write_json(&self->content, self->local.format, &name, buf);
}

static void
ctl_user(Session* self)
{
	auto user_mgr = self->share->user_mgr;

	// upgrade to exclusive lock
	session_lock(self, LOCK_EXCLUSIVE);

	auto stmt = compiler_stmt(&self->compiler);
	switch (stmt->id) {
	case STMT_CREATE_USER:
	{
		auto arg = ast_user_create_of(stmt->ast);
		user_mgr_create(user_mgr, arg->config, arg->if_not_exists);
		break;
	}
	case STMT_DROP_USER:
	{
		auto arg = ast_user_drop_of(stmt->ast);
		user_mgr_drop(user_mgr, &arg->name->string, arg->if_exists);
		break;
	}
	case STMT_ALTER_USER:
	{
		auto arg = ast_user_alter_of(stmt->ast);
		user_mgr_alter(user_mgr, arg->config);
		break;
	}
	default:
		abort();
		break;
	}

	// downgrade back to shared lock to avoid deadlocking
	session_lock(self, LOCK);

	// sync hosts user caches
	host_mgr_sync_users(self->share->host_mgr, &user_mgr->cache);
}

static void
ctl_replica(Session* self)
{
	auto replica_mgr = &self->share->repl->replica_mgr;

	// upgrade to exclusive lock
	session_lock(self, LOCK_EXCLUSIVE);

	auto stmt = compiler_stmt(&self->compiler);
	switch (stmt->id) {
	case STMT_CREATE_REPLICA:
	{
		auto arg = ast_replica_create_of(stmt->ast);
		auto config = replica_config_allocate();
		defer(replica_config_free, config);

		// id
		Uuid id;
		uuid_set(&id, &arg->id->string);
		replica_config_set_id(config, &id);

		// remote
		replica_config_set_remote(config, &arg->remote);
		replica_mgr_create(replica_mgr, config, arg->if_not_exists);
		break;
	}
	case STMT_DROP_REPLICA:
	{
		auto arg = ast_replica_drop_of(stmt->ast);
		Uuid id;
		uuid_set(&id, &arg->id->string);
		replica_mgr_drop(replica_mgr, &id, arg->if_exists);
		break;
	}
	default:
		abort();
		break;
	}

	control_save_state();
}

static void
ctl_repl(Session* self)
{
	auto repl = self->share->repl;

	// upgrade to exclusive lock
	session_lock(self, LOCK_EXCLUSIVE);

	auto stmt = compiler_stmt(&self->compiler);
	switch (stmt->id) {
	case STMT_START_REPL:
	case STMT_STOP_REPL:
	{
		auto arg = ast_repl_ctl_of(stmt->ast);
		if (arg->start)
			repl_start(repl);
		else
			repl_stop(repl);
		break;
	}
	case STMT_SUBSCRIBE:
	case STMT_UNSUBSCRIBE:
	{
		auto arg = ast_repl_subscribe_of(stmt->ast);
		Str* primary_id = NULL;
		if (arg->id)
			primary_id = &arg->id->string;
		repl_subscribe(repl, primary_id);
		break;
	}
	default:
		abort();
		break;
	}

	control_save_state();
}

static void
ctl_checkpoint(Session* self)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_checkpoint_of(stmt->ast);
	session_unlock(self);

	int workers;
	if (arg->workers) {
		workers = arg->workers->integer;
	} else {
		workers = var_int_of(&config()->checkpoint_workers);
		if (workers == 0)
			workers = 1;
	}
	rpc(global()->control->system, RPC_CHECKPOINT, 1, workers);
}

void
session_execute_utility(Session* self)
{
	auto stmt = compiler_stmt(&self->compiler);
	switch (stmt->id) {
	case STMT_SHOW:
		ctl_show(self);
		break;
	case STMT_SET:
		ctl_set(self);
		break;
	case STMT_CREATE_TOKEN:
		ctl_token(self);
		break;
	case STMT_CREATE_USER:
	case STMT_DROP_USER:
	case STMT_ALTER_USER:
		ctl_user(self);
		break;
	case STMT_CREATE_REPLICA:
	case STMT_DROP_REPLICA:
		ctl_replica(self);
		break;
	case STMT_START_REPL:
	case STMT_STOP_REPL:
	case STMT_SUBSCRIBE:
	case STMT_UNSUBSCRIBE:
		ctl_repl(self);
		break;
	case STMT_CHECKPOINT:
		ctl_checkpoint(self);
		break;
	default:
		session_execute_ddl(self);
		break;
	}
}
