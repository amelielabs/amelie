
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
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_append(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2)
		error("append(): expected two or more arguments");
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		error("append(): json array expected");
	if (unlikely(! json_is_array(argv[0].json)))
		error("append(): json array expected");
	auto tz = self->vm->local->timezone;
	value_array_append(self->result, tz, argv[0].json, argv[0].json_size,
	                   self->argc - 1, &argv[1]);
}

hot static void
fn_push(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2)
		error("push(): expected two or more arguments");
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		error("push(): json array expected");
	if (unlikely(! json_is_array(argv[0].json)))
		error("push(): json array expected");
	auto tz = self->vm->local->timezone;
	value_array_push(self->result, tz, argv[0].json, argv[0].json_size,
	                 self->argc - 1, &argv[1]);
}

hot static void
fn_pop(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 1);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		error("pop(): json array expected");
	if (unlikely(! json_is_array(argv[0].json)))
		error("pop(): json array expected");
	value_array_pop(self->result, argv[0].json, argv[0].json_size);
}

hot static void
fn_pop_back(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 1);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		error("pop_back(): json array expected");
	if (unlikely(! json_is_array(argv[0].json)))
		error("pop_back(): json array expected");
	value_array_pop_back(self->result, argv[0].json);
}

hot static void
fn_put(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 3);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_validate_arg(self, 0, TYPE_JSON);
	if (unlikely(! json_is_array(argv[0].json)))
		error("put(): json array expected");
	call_validate_arg(self, 1, TYPE_INT);
	auto tz = self->vm->local->timezone;
	value_array_put(self->result, tz, argv[0].json, argv[1].integer, &argv[2]);
}

hot static void
fn_remove(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_validate_arg(self, 0, TYPE_JSON);
	if (unlikely(! json_is_array(argv[0].json)))
		error("remove(): json array expected");
	call_validate_arg(self, 1, TYPE_INT);
	value_array_remove(self->result, argv[0].json, argv[1].integer);
}

hot static void
fn_set(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 3);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		error("set(): json array expected");
	if (unlikely(! json_is_obj(argv[0].json)))
		error("set(): json object expected");
	call_validate_arg(self, 1, TYPE_STRING);
	auto tz = self->vm->local->timezone;
	update_set(self->result, tz, argv[0].json, &argv[1].string, &argv[2]);
}

hot static void
fn_unset(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		error("unset(): json array expected");
	if (unlikely(! json_is_obj(argv[0].json)))
		error("unset(): json object expected");
	call_validate_arg(self, 1, TYPE_STRING);
	update_unset(self->result, argv[0].json, &argv[1].string);
}

hot static void
fn_has(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != TYPE_JSON))
		error("has(): json array expected");
	if (unlikely(! json_is_obj(argv[0].json)))
		error("has(): json object expected");
	call_validate_arg(self, 1, TYPE_STRING);
	value_obj_has(self->result, argv[0].json, &argv[1].string);
}

FunctionDef fn_json_def[] =
{
	// array
	{ "public", "append",    TYPE_JSON, fn_append,   FN_NONE },
	{ "public", "push_back", TYPE_JSON, fn_append,   FN_NONE },
	{ "public", "push",      TYPE_JSON, fn_push,     FN_NONE },
	{ "public", "pop",       TYPE_JSON, fn_pop,      FN_NONE },
	{ "public", "pop_back",  TYPE_JSON, fn_pop_back, FN_NONE },
	{ "public", "put",       TYPE_JSON, fn_put,      FN_NONE },
	{ "public", "remove",    TYPE_JSON, fn_remove,   FN_NONE },
	// object
	{ "public", "set",       TYPE_JSON, fn_set,      FN_NONE },
	{ "public", "unset",     TYPE_JSON, fn_unset,    FN_NONE },
	{ "public", "has",       TYPE_BOOL, fn_has,      FN_NONE },
	{  NULL,     NULL,       TYPE_NULL, NULL,        FN_NONE }
};
