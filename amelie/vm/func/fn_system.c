
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
#include <amelie_value.h>
#include <amelie_store.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

static void
fn_config(Call* self)
{
	call_validate(self, 0);
	auto buf = vars_list(&config()->vars, &self->vm->local->config.vars);
	value_set_json_buf(self->result, buf);
}

static void
fn_users(Call* self)
{
	call_validate(self, 0);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_USERS, 1, &buf);
	value_set_json_buf(self->result, buf);
}

static void
fn_replicas(Call* self)
{
	call_validate(self, 0);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_REPLICAS, 1, &buf);
	value_set_json_buf(self->result, buf);
}

static void
fn_repl(Call* self)
{
	call_validate(self, 0);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_REPL, 1, &buf);
	value_set_json_buf(self->result, buf);
}

static void
fn_nodes(Call* self)
{
	call_validate(self, 0);
	auto buf = node_mgr_list(&self->vm->db->node_mgr);
	value_set_json_buf(self->result, buf);
}

static void
fn_schemas(Call* self)
{
	call_validate(self, 0);
	auto buf = schema_mgr_list(&self->vm->db->schema_mgr);
	value_set_json_buf(self->result, buf);
}

static void
fn_functions(Call* self)
{
	call_validate(self, 0);
	auto buf = function_mgr_list(self->vm->function_mgr);
	value_set_json_buf(self->result, buf);
}

static void
fn_tables(Call* self)
{
	call_validate(self, 0);
	auto buf = table_mgr_list(&self->vm->db->table_mgr);
	value_set_json_buf(self->result, buf);
}

static void
fn_views(Call* self)
{
	call_validate(self, 0);
	auto buf = view_mgr_list(&self->vm->db->view_mgr);
	value_set_json_buf(self->result, buf);
}

static void
fn_wal(Call* self)
{
	call_validate(self, 0);
	auto buf = wal_status(&self->vm->db->wal);
	value_set_json_buf(self->result, buf);
}

static void
fn_status(Call* self)
{
	call_validate(self, 0);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_STATUS, 1, &buf);
	value_set_json_buf(self->result, buf);
}

FunctionDef fn_system_def[] =
{
	{ "system", "config",      VALUE_JSON, fn_config,    false },
	{ "system", "users",       VALUE_JSON, fn_users,     false },
	{ "system", "replicas",    VALUE_JSON, fn_replicas,  false },
	{ "system", "repl",        VALUE_JSON, fn_repl,      false },
	{ "system", "replication", VALUE_JSON, fn_repl,      false },
	{ "system", "nodes",       VALUE_JSON, fn_nodes,     false },
	{ "system", "schemas",     VALUE_JSON, fn_schemas,   false },
	{ "system", "functions",   VALUE_JSON, fn_functions, false },
	{ "system", "tables",      VALUE_JSON, fn_tables,    false },
	{ "system", "views",       VALUE_JSON, fn_views,     false },
	{ "system", "wal",         VALUE_JSON, fn_wal,       false },
	{ "system", "status",      VALUE_JSON, fn_status,    false },
	{  NULL,     NULL,         VALUE_NULL, NULL,         false }
};
