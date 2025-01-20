
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
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
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
fn_user(Call* self)
{
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_STRING);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_USERS, 2, &buf, &self->argv[0].string);
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
fn_replica(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 1);
	if (argv[0].type == TYPE_STRING)
	{
		Uuid id;
		uuid_set(&id, &argv[0].string);
		Buf* buf;
		rpc(global()->control->system, RPC_SHOW_REPLICAS, 2, &buf, &id);
		value_set_json_buf(self->result, buf);
	} else
	if (argv[0].type == TYPE_UUID)
	{
		Buf* buf;
		rpc(global()->control->system, RPC_SHOW_REPLICAS, 2, &buf, &argv[0].uuid);
		value_set_json_buf(self->result, buf);
	} else
	{
		error("replica(%s): operation type is not supported",
		      type_of(argv[0].type));
	}
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
	auto buf = node_mgr_list(&self->vm->db->node_mgr, NULL);
	value_set_json_buf(self->result, buf);
}

static void
fn_node(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 1);
	if (argv[0].type == TYPE_STRING)
	{
		auto buf = node_mgr_list(&self->vm->db->node_mgr, &self->argv[0].string);
		value_set_json_buf(self->result, buf);
	} else
	if (argv[0].type == TYPE_UUID)
	{
		char uuid_sz[UUID_SZ];
		uuid_get(&argv[0].uuid, uuid_sz, sizeof(uuid_sz));
		Str str;
		str_set(&str, uuid_sz, UUID_SZ - 1);
		auto buf = node_mgr_list(&self->vm->db->node_mgr, &str);
		value_set_json_buf(self->result, buf);
	} else
	{
		error("node(%s): operation type is not supported",
		      type_of(argv[0].type));
	}
}

static void
fn_schemas(Call* self)
{
	call_validate(self, 0);
	auto buf = schema_mgr_list(&self->vm->db->schema_mgr, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_schema(Call* self)
{
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_STRING);
	auto buf = schema_mgr_list(&self->vm->db->schema_mgr, &self->argv[0].string, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_tables(Call* self)
{
	call_validate(self, 0);
	auto buf = table_mgr_list(&self->vm->db->table_mgr, NULL, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_table(Call* self)
{
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_STRING);
	Str name = self->argv[0].string;
	Str schema;
	str_init(&schema);
	if (str_split(&name, &schema, '.'))
		str_advance(&name, str_size(&schema) + 1);
	else
		str_set(&schema, "public", 6);
	auto buf = table_mgr_list(&self->vm->db->table_mgr, &schema, &name, true);
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
	{ "system", "config",      TYPE_JSON, fn_config,    FN_NONE },
	{ "system", "users",       TYPE_JSON, fn_users,     FN_NONE },
	{ "system", "user",        TYPE_JSON, fn_user,      FN_NONE },
	{ "system", "replicas",    TYPE_JSON, fn_replicas,  FN_NONE },
	{ "system", "replica",     TYPE_JSON, fn_replica,   FN_NONE },
	{ "system", "repl",        TYPE_JSON, fn_repl,      FN_NONE },
	{ "system", "replication", TYPE_JSON, fn_repl,      FN_NONE },
	{ "system", "nodes",       TYPE_JSON, fn_nodes,     FN_NONE },
	{ "system", "node",        TYPE_JSON, fn_node,      FN_NONE },
	{ "system", "schemas",     TYPE_JSON, fn_schemas,   FN_NONE },
	{ "system", "schema",      TYPE_JSON, fn_schema,    FN_NONE },
	{ "system", "tables",      TYPE_JSON, fn_tables,    FN_NONE },
	{ "system", "table",       TYPE_JSON, fn_table,     FN_NONE },
	{ "system", "wal",         TYPE_JSON, fn_wal,       FN_NONE },
	{ "system", "status",      TYPE_JSON, fn_status,    FN_NONE },
	{  NULL,     NULL,         TYPE_NULL, NULL,         FN_NONE }
};
