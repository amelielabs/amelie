
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

hot static void
fn_has(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	value_idx_has(self->result, argv[0], argv[1]);
}

hot static void
fn_set(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	value_idx_set(self->result, argv[0], argv[1], argv[2]);
}

hot static void
fn_unset(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	value_idx_unset(self->result, argv[0], argv[1]);
}

hot static void
fn_append(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	value_append(self->result, argv[0], argv[1]);
}

hot static void
fn_sizeof(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	if (arg->type == VALUE_SET)
		value_set_int(self->result, ((Set*)arg->obj)->list_count);
	else
		value_sizeof(self->result, arg);
}

hot static void
fn_string(Call* self)
{
	call_validate(self);
	value_to_string(self->result, self->argv[0], self->vm->local->timezone);
}

hot static void
fn_json(Call* self)
{
	call_validate(self);
	value_to_json(self->result, self->argv[0], self->vm->local->timezone);
}

hot static void
fn_interval(Call* self)
{
	call_validate(self);
	call_validate_arg(self, 0, VALUE_STRING);
	Interval iv;
	interval_init(&iv);
	interval_read(&iv, &self->argv[0]->string);
	value_set_interval(self->result, &iv);
}

hot static void
fn_timestamp(Call* self)
{
	call_validate(self);
	call_validate_arg(self, 0, VALUE_STRING);
	Timestamp ts;
	timestamp_init(&ts);
	timestamp_read(&ts, &self->argv[0]->string);
	auto time = timestamp_of(&ts, self->vm->local->timezone);
	value_set_timestamp(self->result, time);
}

hot static void
fn_generate_series(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	call_validate_arg(self, 0, VALUE_TIMESTAMP);
	call_validate_arg(self, 1, VALUE_TIMESTAMP);
	call_validate_arg(self, 2, VALUE_INTERVAL);

	Timestamp ts;
	timestamp_init(&ts);

	// end
	timestamp_read_value(&ts, argv[1]->integer);
	uint64_t end = timestamp_of(&ts, NULL);

	// pos
	timestamp_init(&ts);
	timestamp_read_value(&ts, argv[0]->integer);
	uint64_t pos = timestamp_of(&ts, NULL);

	auto buf = buf_begin();
	encode_array(buf);
	while (pos <= end)
	{
		encode_timestamp(buf, pos);
		timestamp_add(&ts, &argv[2]->interval);
		pos = timestamp_of(&ts, NULL);
	}
	encode_array_end(buf);
	buf_end(buf);
	value_set_buf(self->result, buf);
}

hot static void
fn_time_bucket(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	call_validate_arg(self, 0, VALUE_INTERVAL);
	call_validate_arg(self, 1, VALUE_TIMESTAMP);
	auto iv = &argv[0]->interval;
	if (iv->m != 0)
		error("time_bucket(): month intervals are not supported");
	uint64_t span = iv->us + iv->d * 86400000000ULL;
	uint64_t ts = argv[1]->integer / span * span;
	value_set_timestamp(self->result, ts);
}

hot static void
fn_now(Call* self)
{
	call_validate(self);
	value_set_timestamp(self->result, time_us());
}

hot static void
fn_error(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_STRING);
	error("%.*s", str_size(&arg->string), str_of(&arg->string));
}

void
func_setup(FunctionMgr* mgr)
{
	struct
	{
		const char*  schema;
		const char*  name;
		FunctionMain function;
		int          argc;
	} def[] =
	{
		// system
		{ "system", "config",          fn_config,          0 },
		{ "system", "users",           fn_users,           0 },
		{ "system", "replicas",        fn_replicas,        0 },
		{ "system", "repl",            fn_repl,            0 },
		{ "system", "replication",     fn_repl,            0 },
		{ "system", "nodes",           fn_nodes,           0 },
		{ "system", "schemas",         fn_schemas,         0 },
		{ "system", "functions",       fn_functions,       0 },
		{ "system", "tables",          fn_tables,          0 },
		{ "system", "views",           fn_views,           0 },
		{ "system", "wal",             fn_wal,             0 },
		// public
		{ "public", "has",             fn_has,             2 },
		{ "public", "set",             fn_set,             3 },
		{ "public", "unset",           fn_unset,           2 },
		{ "public", "append",          fn_append,          2 },
		{ "public", "sizeof",          fn_sizeof,          1 },
		{ "public", "string",          fn_string,          1 },
		{ "public", "json",            fn_json,            1 },
		{ "public", "interval",        fn_interval,        1 },
		{ "public", "timestamp",       fn_timestamp,       1 },
		{ "public", "generate_series", fn_generate_series, 3 },
		{ "public", "time_bucket",     fn_time_bucket,     2 },
		{ "public", "now",             fn_now,             0 },
		{ "public", "error",           fn_error,           1 },
		{  NULL,     NULL,             NULL,               0 }
	};
	for (int i = 0; def[i].name; i++)
	{
		auto func = function_allocate(def[i].schema, def[i].name,
		                              def[i].argc,
		                              def[i].function);
		function_mgr_add(mgr, func);
	}
}
