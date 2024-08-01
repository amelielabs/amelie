
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
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_call.h>

static void
fn_config(Call* self)
{
	call_validate(self);
	auto buf = config_list(config(), &self->vm->local->config);
	value_set_buf(self->result, buf);
}

static void
fn_users(Call* self)
{
	call_validate(self);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_USERS, 1, &buf);
	value_set_buf(self->result, buf);
}

static void
fn_replicas(Call* self)
{
	call_validate(self);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_REPLICAS, 1, &buf);
	value_set_buf(self->result, buf);
}

static void
fn_repl(Call* self)
{
	call_validate(self);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_REPL, 1, &buf);
	value_set_buf(self->result, buf);
}

static void
fn_nodes(Call* self)
{
	call_validate(self);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_NODES, 1, &buf);
	value_set_buf(self->result, buf);
}

static void
fn_schemas(Call* self)
{
	call_validate(self);
	auto buf = schema_mgr_list(&self->vm->db->schema_mgr);
	value_set_buf(self->result, buf);
}

static void
fn_functions(Call* self)
{
	call_validate(self);
	auto buf = function_mgr_list(self->vm->function_mgr);
	value_set_buf(self->result, buf);
}

static void
fn_tables(Call* self)
{
	call_validate(self);
	auto buf = table_mgr_list(&self->vm->db->table_mgr);
	value_set_buf(self->result, buf);
}

static void
fn_views(Call* self)
{
	call_validate(self);
	auto buf = view_mgr_list(&self->vm->db->view_mgr);
	value_set_buf(self->result, buf);
}

static void
fn_wal(Call* self)
{
	call_validate(self);
	auto buf = wal_show(&self->vm->db->wal);
	value_set_buf(self->result, buf);
}

FunctionDef fn_system[] =
{
	{ "system", "config",      fn_config,    0 },
	{ "system", "users",       fn_users,     0 },
	{ "system", "replicas",    fn_replicas,  0 },
	{ "system", "repl",        fn_repl,      0 },
	{ "system", "replication", fn_repl,      0 },
	{ "system", "nodes",       fn_nodes,     0 },
	{ "system", "schemas",     fn_schemas,   0 },
	{ "system", "functions",   fn_functions, 0 },
	{ "system", "tables",      fn_tables,    0 },
	{ "system", "views",       fn_views,     0 },
	{ "system", "wal",         fn_wal,       0 },
	{  NULL,     NULL,         NULL,         0 }
};
