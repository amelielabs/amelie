
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
#include <amelie_heap.h>
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

static void
fn_config(Call* self)
{
	call_expect(self, 0);
	auto buf = opts_list(&config()->opts);
	value_set_json_buf(self->result, buf);
}

static void
fn_state(Call* self)
{
	call_expect(self, 0);
	auto buf = db_state(self->mgr->db);
	value_set_json_buf(self->result, buf);
}

static void
fn_users(Call* self)
{
	call_expect(self, 0);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_USERS, 1, &buf);
	value_set_json_buf(self->result, buf);
}

static void
fn_user(Call* self)
{
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_STRING);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_USERS, 2, &buf, &self->argv[0].string);
	value_set_json_buf(self->result, buf);
}

static void
fn_replicas(Call* self)
{
	call_expect(self, 0);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_REPLICAS, 1, &buf);
	value_set_json_buf(self->result, buf);
}

static void
fn_replica(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 1);
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
	} else {
		call_unsupported(self, 0);
	}
}

static void
fn_repl(Call* self)
{
	call_expect(self, 0);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_REPL, 1, &buf);
	value_set_json_buf(self->result, buf);
}

static void
fn_schemas(Call* self)
{
	call_expect(self, 0);
	auto buf = schema_mgr_list(&self->mgr->db->schema_mgr, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_schema(Call* self)
{
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_STRING);
	auto buf = schema_mgr_list(&self->mgr->db->schema_mgr, &self->argv[0].string, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_tables(Call* self)
{
	call_expect(self, 0);
	auto buf = table_mgr_list(&self->mgr->db->table_mgr, NULL, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_table(Call* self)
{
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_STRING);
	Str name = self->argv[0].string;
	Str schema;
	str_init(&schema);
	if (str_split(&name, &schema, '.'))
		str_advance(&name, str_size(&schema) + 1);
	else
		str_set(&schema, "public", 6);
	auto buf = table_mgr_list(&self->mgr->db->table_mgr, &schema, &name, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_procedures(Call* self)
{
	call_expect(self, 0);
	auto buf = proc_mgr_list(&self->mgr->db->proc_mgr, NULL, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_procedure(Call* self)
{
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_STRING);
	Str name = self->argv[0].string;
	Str schema;
	str_init(&schema);
	if (str_split(&name, &schema, '.'))
		str_advance(&name, str_size(&schema) + 1);
	else
		str_set(&schema, "public", 6);
	auto buf = proc_mgr_list(&self->mgr->db->proc_mgr, &schema, &name, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_wal(Call* self)
{
	call_expect(self, 0);
	auto buf = wal_status(&self->mgr->db->wal_mgr.wal);
	value_set_json_buf(self->result, buf);
}

static void
fn_metrics(Call* self)
{
	call_expect(self, 0);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_METRICS, 1, &buf);
	value_set_json_buf(self->result, buf);
}

void
fn_system_register(FunctionMgr* self)
{
	// system.config()
	Function* func;
	func = function_allocate(TYPE_JSON, "system", "config", fn_config);
	function_mgr_add(self, func);

	// system.state()
	func = function_allocate(TYPE_JSON, "system", "state", fn_state);
	function_mgr_add(self, func);

	// system.users()
	func = function_allocate(TYPE_JSON, "system", "users", fn_users);
	function_mgr_add(self, func);

	// system.user()
	func = function_allocate(TYPE_JSON, "system", "user", fn_user);
	function_mgr_add(self, func);

	// system.replicas()
	func = function_allocate(TYPE_JSON, "system", "replicas", fn_replicas);
	function_mgr_add(self, func);

	// system.replica()
	func = function_allocate(TYPE_JSON, "system", "replica", fn_replica);
	function_mgr_add(self, func);

	// system.replica(uuid)
	func = function_allocate(TYPE_JSON, "system", "replica", fn_replica);
	function_mgr_add(self, func);

	// system.repl()
	func = function_allocate(TYPE_JSON, "system", "repl", fn_repl);
	function_mgr_add(self, func);

	// system.replication()
	func = function_allocate(TYPE_JSON, "system", "replication", fn_repl);
	function_mgr_add(self, func);

	// system.schemas()
	func = function_allocate(TYPE_JSON, "system", "schemas", fn_schemas);
	function_mgr_add(self, func);

	// system.schema()
	func = function_allocate(TYPE_JSON, "system", "schema", fn_schema);
	function_mgr_add(self, func);

	// system.tables()
	func = function_allocate(TYPE_JSON, "system", "tables", fn_tables);
	function_mgr_add(self, func);

	// system.table()
	func = function_allocate(TYPE_JSON, "system", "table", fn_table);
	function_mgr_add(self, func);

	// system.procedures()
	func = function_allocate(TYPE_JSON, "system", "procedures", fn_procedures);
	function_mgr_add(self, func);

	// system.procedure()
	func = function_allocate(TYPE_JSON, "system", "procedure", fn_procedure);
	function_mgr_add(self, func);

	// system.wal()
	func = function_allocate(TYPE_JSON, "system", "wal", fn_wal);
	function_mgr_add(self, func);

	// system.metrics()
	func = function_allocate(TYPE_JSON, "system", "metrics", fn_metrics);
	function_mgr_add(self, func);
}
