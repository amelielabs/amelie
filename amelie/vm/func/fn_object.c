
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
#include <amelie_data.h>
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
	if (unlikely(argv[0].type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != VALUE_JSON))
		error("append(): json array expected");
	if (unlikely(! data_is_array(argv[0].data)))
		error("append(): json array expected");
	value_array_append(self->result, argv[0].data, argv[0].data_size,
	                   self->argc - 1, &argv[1]);
}

hot static void
fn_push(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2)
		error("push(): expected two or more arguments");
	if (unlikely(argv[0].type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != VALUE_JSON))
		error("push(): json array expected");
	if (unlikely(! data_is_array(argv[0].data)))
		error("push(): json array expected");
	value_array_push(self->result, argv[0].data, argv[0].data_size,
	                 self->argc - 1, &argv[1]);
}

hot static void
fn_pop(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 1);
	if (unlikely(argv[0].type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != VALUE_JSON))
		error("pop(): json array expected");
	if (unlikely(! data_is_array(argv[0].data)))
		error("pop(): json array expected");
	value_array_pop(self->result, argv[0].data, argv[0].data_size);
}

hot static void
fn_pop_back(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 1);
	if (unlikely(argv[0].type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != VALUE_JSON))
		error("pop_back(): json array expected");
	if (unlikely(! data_is_array(argv[0].data)))
		error("pop_back(): json array expected");
	value_array_pop_back(self->result, argv[0].data);
}

hot static void
fn_put(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 3);
	if (unlikely(argv[0].type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_validate_arg(self, 0, VALUE_JSON);
	if (unlikely(! data_is_array(argv[0].data)))
		error("put(): json array expected");
	call_validate_arg(self, 1, VALUE_INT);
	value_array_put(self->result, argv[0].data, argv[1].integer, &argv[2]);
}

hot static void
fn_remove(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_validate_arg(self, 0, VALUE_JSON);
	if (unlikely(! data_is_array(argv[0].data)))
		error("remove(): json array expected");
	call_validate_arg(self, 1, VALUE_INT);
	value_array_remove(self->result, argv[0].data, argv[1].integer);
}

hot static void
fn_set(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 3);
	if (unlikely(argv[0].type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != VALUE_JSON))
		error("set(): json array expected");
	if (unlikely(! data_is_obj(argv[0].data)))
		error("set(): json object expected");
	call_validate_arg(self, 1, VALUE_STRING);
	update_set(self->result, argv[0].data, &argv[1].string, &argv[2]);
}

hot static void
fn_unset(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != VALUE_JSON))
		error("unset(): json array expected");
	if (unlikely(! data_is_obj(argv[0].data)))
		error("unset(): json object expected");
	call_validate_arg(self, 1, VALUE_STRING);
	update_unset(self->result, argv[0].data, &argv[1].string);
}

hot static void
fn_has(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(argv[0].type != VALUE_JSON))
		error("has(): json array expected");
	if (unlikely(! data_is_obj(argv[0].data)))
		error("has(): json object expected");
	call_validate_arg(self, 1, VALUE_STRING);
	value_obj_has(self->result, argv[0].data, &argv[1].string);
}

FunctionDef fn_object_def[] =
{
	// array
	{ "public", "append",    VALUE_JSON, fn_append,   false },
	{ "public", "push_back", VALUE_JSON, fn_append,   false },
	{ "public", "push",      VALUE_JSON, fn_push,     false },
	{ "public", "pop",       VALUE_JSON, fn_pop,      false },
	{ "public", "pop_back",  VALUE_JSON, fn_pop_back, false },
	{ "public", "put",       VALUE_JSON, fn_put,      false },
	{ "public", "remove",    VALUE_JSON, fn_remove,   false },
	// object
	{ "public", "set",       VALUE_JSON, fn_set,      false },
	{ "public", "unset",     VALUE_JSON, fn_unset,    false },
	{ "public", "has",       VALUE_BOOL, fn_has,      false },
	{  NULL,     NULL,       VALUE_NONE, NULL,        false }
};
