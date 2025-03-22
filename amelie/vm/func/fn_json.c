
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

hot static void
fn_append(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2)
		call_error(self, "expected two or more arguments");
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		call_error_arg(self, 0, "json array expected");
	if (unlikely(! json_is_array(argv[0].json)))
		call_error_arg(self, 0, "json array expected");
	auto tz = self->mgr->local->timezone;
	value_array_append(self->result, tz, argv[0].json, argv[0].json_size,
	                   self->argc - 1, &argv[1]);
}

hot static void
fn_push(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2)
		call_error(self, "expected two or more arguments");
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		call_error_arg(self, 0, "json array expected");
	if (unlikely(! json_is_array(argv[0].json)))
		call_error_arg(self, 0, "json array expected");
	auto tz = self->mgr->local->timezone;
	value_array_push(self->result, tz, argv[0].json, argv[0].json_size,
	                 self->argc - 1, &argv[1]);
}

hot static void
fn_pop(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 1);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		call_error_arg(self, 0, "json array expected");
	if (unlikely(! json_is_array(argv[0].json)))
		call_error_arg(self, 0, "json array expected");
	value_array_pop(self->result, argv[0].json, argv[0].json_size);
}

hot static void
fn_pop_back(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 1);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		call_error_arg(self, 0, "json array expected");
	if (unlikely(! json_is_array(argv[0].json)))
		call_error_arg(self, 0, "json array expected");
	value_array_pop_back(self->result, argv[0].json);
}

hot static void
fn_put(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 3);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_expect_arg(self, 0, TYPE_JSON);
	if (unlikely(! json_is_array(argv[0].json)))
		call_error_arg(self, 0, "json array expected");
	call_expect_arg(self, 1, TYPE_INT);
	auto tz = self->mgr->local->timezone;
	value_array_put(self->result, tz, argv[0].json, argv[1].integer, &argv[2]);
}

hot static void
fn_remove(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_expect_arg(self, 0, TYPE_JSON);
	if (unlikely(! json_is_array(argv[0].json)))
		call_error_arg(self, 0, "json array expected");
	call_expect_arg(self, 1, TYPE_INT);
	value_array_remove(self->result, argv[0].json, argv[1].integer);
}

hot static void
fn_set(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 3);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		call_error_arg(self, 0, "json object expected");
	if (unlikely(! json_is_obj(argv[0].json)))
		call_error_arg(self, 0, "json object expected");
	call_expect_arg(self, 1, TYPE_STRING);
	auto tz = self->mgr->local->timezone;
	update_set(self->result, tz, argv[0].json, &argv[1].string, &argv[2]);
}

hot static void
fn_unset(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		call_error_arg(self, 0, "json object expected");
	if (unlikely(! json_is_obj(argv[0].json)))
		call_error_arg(self, 0, "json object expected");
	call_expect_arg(self, 1, TYPE_STRING);
	update_unset(self->result, argv[0].json, &argv[1].string);
}

hot static void
fn_has(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		call_error_arg(self, 0, "json object expected");
	if (unlikely(! json_is_obj(argv[0].json)))
		call_error_arg(self, 0, "json object expected");
	call_expect_arg(self, 1, TYPE_STRING);
	value_obj_has(self->result, argv[0].json, &argv[1].string);
}

void
fn_json_register(FunctionMgr* self)
{
	// public.append()
	Function* func;
	func = function_allocate(TYPE_JSON, "public", "append", fn_append);
	function_mgr_add(self, func);

	// public.push_back()
	func = function_allocate(TYPE_JSON, "public", "push_back", fn_append);
	function_mgr_add(self, func);

	// public.push()
	func = function_allocate(TYPE_JSON, "public", "push", fn_push);
	function_mgr_add(self, func);

	// public.pop()
	func = function_allocate(TYPE_JSON, "public", "pop", fn_pop);
	function_mgr_add(self, func);

	// public.pop_back()
	func = function_allocate(TYPE_JSON, "public", "pop_back", fn_pop_back);
	function_mgr_add(self, func);

	// public.put()
	func = function_allocate(TYPE_JSON, "public", "put", fn_put);
	function_mgr_add(self, func);

	// public.remove()
	func = function_allocate(TYPE_JSON, "public", "remove", fn_remove);
	function_mgr_add(self, func);

	// public.set()
	func = function_allocate(TYPE_JSON, "public", "set", fn_set);
	function_mgr_add(self, func);

	// public.unset()
	func = function_allocate(TYPE_JSON, "public", "unset", fn_unset);
	function_mgr_add(self, func);

	// public.has()
	func = function_allocate(TYPE_BOOL, "public", "has", fn_has);
	function_mgr_add(self, func);
}
