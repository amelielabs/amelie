
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
#include <sonata_backup.h>
#include <sonata_repl.h>
#include <sonata_cluster.h>
#include <sonata_frontend.h>
#include <sonata_session.h>
#include <sonata_main.h>

static Buf*
ctl_show(System* self, Stmt* stmt)
{
	auto arg  = ast_show_of(stmt->ast);
	auto name = &arg->expr->string;
	Buf* buf  = NULL;
	if (str_compare_raw(name, "users", 5))
		buf = user_mgr_list(&self->user_mgr);
	else
	if (str_compare_raw(name, "nodes", 5))
		buf = cluster_list(&self->cluster);
	else
	if (str_compare_raw(name, "cluster", 7))
		buf = cluster_list(&self->cluster);
	else
	if (str_compare_raw(name, "wal", 3))
		buf = wal_show(&self->db.wal);
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
		buf = buf_begin();
		var_encode(var, buf);
		buf_end(buf);
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
	if (! var_is(var, VAR_E))
		control_save_config();
}

static void
ctl_create_user(System* self, Stmt* stmt)
{
	auto arg = ast_user_create_of(stmt->ast);
	user_mgr_create(&self->user_mgr, arg->config, arg->if_not_exists);
	frontend_mgr_sync(&self->frontend_mgr, &self->user_mgr.cache);
}

static void
ctl_drop_user(System* self, Stmt* stmt)
{
	auto arg = ast_user_drop_of(stmt->ast);
	user_mgr_drop(&self->user_mgr, &arg->name->string, arg->if_exists);
	frontend_mgr_sync(&self->frontend_mgr, &self->user_mgr.cache);
}

static void
ctl_alter_user(System* self, Stmt* stmt)
{
	auto arg = ast_user_alter_of(stmt->ast);
	user_mgr_alter(&self->user_mgr, arg->config);
	frontend_mgr_sync(&self->frontend_mgr, &self->user_mgr.cache);
}

static void
ctl_create_node(System* self, Stmt* stmt)
{
	auto arg = ast_node_create_of(stmt->ast);

	// get exclusive session lock
	frontend_mgr_lock(&self->frontend_mgr);

	Exception e;
	if (enter(&e))
	{
		auto config = node_config_allocate();
		guard(node_config_free, config);

		// id
		Uuid id;
		if (arg->id)
			uuid_from_string(&id, &arg->id->string);
		else
			uuid_generate(&id, global()->random);
		node_config_set_id(config, &id);

		cluster_create(&self->cluster, config, arg->if_not_exists);
		control_save_config();
	}

	frontend_mgr_unlock(&self->frontend_mgr);
	if (leave(&e))
		rethrow();
}

static void
ctl_drop_node(System* self, Stmt* stmt)
{
	auto arg = ast_node_drop_of(stmt->ast);

	// get exclusive session lock
	frontend_mgr_lock(&self->frontend_mgr);

	Exception e;
	if (enter(&e))
	{
		Uuid id;
		uuid_from_string(&id, &arg->id->string);
		cluster_drop(&self->cluster, &id, arg->if_exists);
		control_save_config();
	}

	frontend_mgr_unlock(&self->frontend_mgr);
	if (leave(&e))
		rethrow();
}

static void
ctl_gc(System* self)
{
	checkpoint_mgr_gc(&self->db.checkpoint_mgr);

	wal_gc(&self->db.wal, config_checkpoint());
}

static void
ctl_checkpoint(System* self, Stmt* stmt)
{
	auto arg = ast_checkpoint_of(stmt->ast);

	// get an exclusive session lock
	frontend_mgr_lock(&self->frontend_mgr);
	bool locked = true;

	// prepare checkpoint
	uint64_t lsn = config_lsn();
	if (lsn == config_checkpoint())
	{
		frontend_mgr_unlock(&self->frontend_mgr);
		return;
	}
	int workers = self->cluster.list_count;
	if (arg->workers)
		workers = arg->workers->integer;

	Checkpoint cp;
	checkpoint_init(&cp);

	Exception e;
	if (enter(&e))
	{
		// prepare checkpoint
		checkpoint_begin(&cp, &self->catalog_mgr, lsn, workers);

		// prepare partitions
		list_foreach(&self->db.table_mgr.mgr.list)
		{
			auto table = table_of(list_at(Handle, link));
			checkpoint_add(&cp, &table->part_mgr);
		}

		// run workers and create snapshots
		checkpoint_run(&cp);

		// unlock frontends
		frontend_mgr_unlock(&self->frontend_mgr);
		locked = false;

		// wait for completion
		checkpoint_wait(&cp, &self->db.checkpoint_mgr);
	}

	if (locked)
		frontend_mgr_unlock(&self->frontend_mgr);

	checkpoint_free(&cp);
	if (leave(&e))
	{
		// rpc by unlock changes code
		so_self()->error.code = ERROR;
		rethrow();
	}

	// run system cleanup
	ctl_gc(self);
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
	case STMT_CREATE_NODE:
		ctl_create_node(self, stmt);
		break;
	case STMT_DROP_NODE:
		ctl_drop_node(self, stmt);
		break;
	case STMT_CHECKPOINT:
		ctl_checkpoint(self, stmt);
		break;
	default:
		system_ddl(self, session, stmt);
		break;
	}
	return NULL;
}
