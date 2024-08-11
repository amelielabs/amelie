
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
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
#include <amelie_aggr.h>
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

static Buf*
ctl_show(Session* self)
{
	auto stmt  = compiler_stmt(&self->compiler);
	auto arg   = ast_show_of(stmt->ast);
	auto name  = &arg->expr->string;
	auto share = self->share;
	Buf* buf   = NULL;
	if (str_compare_raw(name, "users", 5))
		buf = user_mgr_list(share->user_mgr);
	else
	if (str_compare_raw(name, "replicas", 8))
		buf = replica_mgr_list(&share->repl->replica_mgr);
	else
	if (str_compare_raw(name, "nodes", 5))
		buf = cluster_list(share->cluster);
	else
	if (str_compare_raw(name, "cluster", 7))
		buf = cluster_list(share->cluster);
	else
	if (str_compare_raw(name, "repl", 4) ||
	    str_compare_raw(name, "replication", 11))
		buf = repl_show(share->repl);
	else
	if (str_compare_raw(name, "wal", 3))
		buf = wal_show(&share->db->wal);
	else
	if (str_compare_raw(name, "schemas", 7))
		buf = schema_mgr_list(&share->db->schema_mgr);
	else
	if (str_compare_raw(name, "functions", 9))
		buf = function_mgr_list(share->function_mgr);
	else
	if (str_compare_raw(name, "tables", 6))
		buf = table_mgr_list(&share->db->table_mgr);
	else
	if (str_compare_raw(name, "views", 5))
		buf = view_mgr_list(&share->db->view_mgr);
	else
	if (str_compare_raw(name, "all", 3))
		buf = config_list(global()->config, &self->local.config);
	else
	{
		auto var = config_find(global()->config, name);
		if (var && var_is(var, VAR_S))
			var = NULL;
		if (unlikely(var == NULL))
			error("SHOW name: '%.*s' not found", str_size(name),
			      str_of(name));
		if (var_is(var, VAR_L))
			var = config_local_find(&self->local.config, name);
		buf = buf_begin();
		var_encode(var, buf);
		buf_end(buf);
	}
	return buf;
}

static void
ctl_set(Session* self)
{
	auto stmt = compiler_stmt(&self->compiler);
	auto arg  = ast_set_of(stmt->ast);
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

	// upgrade to exclusive lock, if var requires config update
	if (! var_is(var, VAR_E))
		session_lock(self, LOCK_EXCLUSIVE);

	// local variable
	auto value = arg->value;
	if (var_is(var, VAR_L))
	{
		auto local = &self->local;
		var = config_local_find(&local->config, name);
		assert(var);

		// validate and set timezone first
		if (var == &local->config.timezone)
		{
			if (value->id != KSTRING)
				error("set '%.*s': string value expected", str_size(name),
				      str_of(name));
			auto timezone = timezone_mgr_find(global()->timezone_mgr, &value->string);
			if (! timezone)
				error("set '%.*s': unable to find timezone '%.*s'", str_size(name),
				      str_of(name), str_size(&value->string),
				      str_of(&value->string));

			// set new timezone
			local->timezone = timezone;
		}
	}

	// set value
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
	if (! var_is(var, VAR_E))
		control_save_config();
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

	// sync frontends user caches
	frontend_mgr_sync(self->share->frontend_mgr, &user_mgr->cache);
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
		guard(replica_config_free, config);

		// id
		Uuid id;
		uuid_from_string(&id, &arg->id->string);
		replica_config_set_id(config, &id);

		// uri
		replica_config_set_uri(config, &arg->uri->string);
		replica_mgr_create(replica_mgr, config, arg->if_not_exists);
		break;
	}
	case STMT_DROP_REPLICA:
	{
		auto arg = ast_replica_drop_of(stmt->ast);
		Uuid id;
		uuid_from_string(&id, &arg->id->string);
		replica_mgr_drop(replica_mgr, &id, arg->if_exists);
		break;
	}
	default:
		abort();
		break;
	}

	control_save_config();
}

static void
ctl_node(Session* self)
{
	auto cluster = self->share->cluster;

	// upgrade to exclusive lock
	session_lock(self, LOCK_EXCLUSIVE);

	auto stmt = compiler_stmt(&self->compiler);
	switch (stmt->id) {
	case STMT_CREATE_NODE:
	{
		auto arg = ast_node_create_of(stmt->ast);
		auto config = node_config_allocate();
		guard(node_config_free, config);

		// id
		Uuid id;
		if (arg->id)
			uuid_from_string(&id, &arg->id->string);
		else
			uuid_generate(&id, global()->random);
		node_config_set_id(config, &id);

		cluster_create(cluster, config, arg->if_not_exists);
		break;
	}
	case STMT_DROP_NODE:
	{
		auto arg = ast_node_drop_of(stmt->ast);
		Uuid id;
		uuid_from_string(&id, &arg->id->string);
		cluster_drop(cluster, &id, arg->if_exists);
		break;
	}
	default:
		abort();
		break;
	}

	control_save_config();
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
	case STMT_PROMOTE:
	{
		auto arg = ast_repl_promote_of(stmt->ast);
		Str* primary_id = NULL;
		if (arg->id)
			primary_id = &arg->id->string;
		repl_promote(repl, primary_id);
		break;
	}
	default:
		abort();
		break;
	}

	control_save_config();
}

static void
ctl_checkpoint(Session* self)
{
	auto stmt  = compiler_stmt(&self->compiler);
	auto arg   = ast_checkpoint_of(stmt->ast);
	auto share = self->share;

	// todo: concurrent checkpoint lock required

	// upgrade to exclusive lock
	session_lock(self, LOCK_EXCLUSIVE);

	// prepare checkpoint
	uint64_t lsn = config_lsn();
	if (lsn == config_checkpoint())
		return;
	int workers = share->cluster->list_count;
	if (arg->workers)
		workers = arg->workers->integer;

	auto cp_mgr = &share->db->checkpoint_mgr;
	Checkpoint cp;
	checkpoint_init(&cp, cp_mgr);

	Exception e;
	if (enter(&e))
	{
		// prepare checkpoint
		checkpoint_begin(&cp, lsn, workers);

		// run workers and create snapshots
		checkpoint_run(&cp);

		// unlock frontends
		session_unlock(self);

		// wait for completion
		checkpoint_wait(&cp);
	}

	checkpoint_free(&cp);
	if (leave(&e))
		rethrow();

	// run system cleanup
	cp_mgr->iface->complete(cp_mgr->iface_arg);
}

void
session_execute_utility(Session* self)
{
	Buf* buf  = NULL;
	auto stmt = compiler_stmt(&self->compiler);
	switch (stmt->id) {
	case STMT_SHOW:
		buf = ctl_show(self);
		break;
	case STMT_SET:
		ctl_set(self);
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
	case STMT_CREATE_NODE:
	case STMT_DROP_NODE:
		ctl_node(self);
		break;
	case STMT_START_REPL:
	case STMT_STOP_REPL:
	case STMT_PROMOTE:
		ctl_repl(self);
		break;
	case STMT_CHECKPOINT:
		ctl_checkpoint(self);
		break;

	default:
		session_execute_ddl(self);
		break;
	}

	if (buf)
	{
		guard_buf(buf);
		body_add_buf(&self->client->reply.content, buf,
		              self->local.timezone);
	}
}
