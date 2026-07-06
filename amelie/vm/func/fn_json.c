
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_commit.h>
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
		call_error_at(self, 0, "json array expected");
	if (unlikely(! data_is_array(argv[0].json)))
		call_error_at(self, 0, "json array expected");
	auto tz = self->local->timezone;
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
		call_error_at(self, 0, "json array expected");
	if (unlikely(! data_is_array(argv[0].json)))
		call_error_at(self, 0, "json array expected");
	auto tz = self->local->timezone;
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
		call_error_at(self, 0, "json array expected");
	if (unlikely(! data_is_array(argv[0].json)))
		call_error_at(self, 0, "json array expected");
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
		call_error_at(self, 0, "json array expected");
	if (unlikely(! data_is_array(argv[0].json)))
		call_error_at(self, 0, "json array expected");
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
	call_arg(self, 0, TYPE_JSON);
	if (unlikely(! data_is_array(argv[0].json)))
		call_error_at(self, 0, "json array expected");
	call_arg(self, 1, TYPE_INT);
	auto tz = self->local->timezone;
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
	call_arg(self, 0, TYPE_JSON);
	if (unlikely(! data_is_array(argv[0].json)))
		call_error_at(self, 0, "json array expected");
	call_arg(self, 1, TYPE_INT);
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
		call_error_at(self, 0, "json object expected");
	if (unlikely(! data_is_obj(argv[0].json)))
		call_error_at(self, 0, "json object expected");
	call_arg(self, 1, TYPE_STRING);
	auto tz = self->local->timezone;
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
		call_error_at(self, 0, "json object expected");
	if (unlikely(! data_is_obj(argv[0].json)))
		call_error_at(self, 0, "json object expected");
	call_arg(self, 1, TYPE_STRING);
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
		call_error_at(self, 0, "json object expected");
	if (unlikely(! data_is_obj(argv[0].json)))
		call_error_at(self, 0, "json object expected");
	call_arg(self, 1, TYPE_STRING);
	value_obj_has(self->result, argv[0].json, &argv[1].string);
}

void
fn_json_register(Functions* self)
{
	// append()
	Function* func;
	func = function_allocate(TYPE_JSON, "append", fn_append);
	functions_add(self, func);

	// push_back()
	func = function_allocate(TYPE_JSON, "push_back", fn_append);
	functions_add(self, func);

	// push()
	func = function_allocate(TYPE_JSON, "push", fn_push);
	functions_add(self, func);

	// pop()
	func = function_allocate(TYPE_JSON, "pop", fn_pop);
	functions_add(self, func);

	// pop_back()
	func = function_allocate(TYPE_JSON, "pop_back", fn_pop_back);
	functions_add(self, func);

	// put()
	func = function_allocate(TYPE_JSON, "put", fn_put);
	functions_add(self, func);

	// remove()
	func = function_allocate(TYPE_JSON, "remove", fn_remove);
	functions_add(self, func);

	// set()
	func = function_allocate(TYPE_JSON, "set", fn_set);
	functions_add(self, func);

	// unset()
	func = function_allocate(TYPE_JSON, "unset", fn_unset);
	functions_add(self, func);

	// has()
	func = function_allocate(TYPE_BOOL, "has", fn_has);
	functions_add(self, func);
}
