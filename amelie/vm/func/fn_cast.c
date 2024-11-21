
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

static void
fn_type(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self, 1);
	Str string;
	str_set_cstr(&string, type_of(arg.type));
	value_set_string(self->result, &string, NULL);
}

hot static void
fn_string(Call* self)
{
	call_validate(self, 1);
	auto arg = self->argv[0];
	if (unlikely(arg.type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	auto data = buf_create();
	switch (arg.type) {
	case TYPE_STRING:
		buf_printf(data, "%.*s", str_size(&arg.string), str_of(&arg.string));
		break;
	case TYPE_INTERVAL:
	{
		buf_reserve(data, 512);
		int size = interval_write(&arg.interval, (char*)data->position, 512);
		buf_advance(data, size);
		break;

	case TYPE_TIMESTAMP:
	{
		buf_reserve(data, 128);
		int size = timestamp_write(arg.integer, self->vm->local->timezone, (char*)data->position, 128);
		buf_advance(data, size);
		break;
	}
	}
	default:
		value_export(&arg, self->vm->local->timezone, false, data);
		break;
	}
	Str string;
	str_init(&string);
	str_set(&string, (char*)data->start, buf_size(data));
	value_set_string(self->result, &string, data);
}

hot static void
fn_int(Call* self)
{
	call_validate(self, 1);
	auto arg = &self->argv[0];
	if (arg->type == TYPE_JSON)
	{
		value_decode(self->result, arg->json, NULL);
		arg = self->result;
	}

	int64_t value = 0;
	switch (arg->type) {
	case TYPE_DOUBLE:
		value = arg->dbl;
		break;
	case TYPE_BOOL:
	case TYPE_INT:
	case TYPE_TIMESTAMP:
		value = arg->integer;
		break;
	case TYPE_STRING:
		if (str_toint(&arg->string, &value) == -1)
			error("int(): failed to cast string");
		break;
	case TYPE_NULL:
		value_set_null(self->result);
		return;
	default:
		error("int(%s): operation type is not supported",
		      type_of(arg->type));
		break;
	}
	value_set_int(self->result, value);
}

hot static void
fn_bool(Call* self)
{
	call_validate(self, 1);
	auto arg = &self->argv[0];
	if (arg->type == TYPE_JSON)
	{
		value_decode(self->result, arg->json, NULL);
		arg = self->result;
	}

	bool value = false;
	switch (arg->type) {
	case TYPE_DOUBLE:
		value = arg->dbl > 0.0;
		break;
	case TYPE_BOOL:
	case TYPE_INT:
	case TYPE_TIMESTAMP:
		value = arg->integer > 0;
		break;
	case TYPE_INTERVAL:
		value = (arg->interval.us + arg->interval.d + arg->interval.m) > 0;
		break;
	case TYPE_STRING:
		value = str_size(&arg->string) > 0;
		break;
	case TYPE_NULL:
		value_set_null(self->result);
		return;
	default:
		error("bool(%s): operation type is not supported",
		      type_of(arg->type));
		break;
	}
	value_set_bool(self->result, value);
}

hot static void
fn_double(Call* self)
{
	call_validate(self, 1);
	auto arg = &self->argv[0];
	if (arg->type == TYPE_JSON)
	{
		value_decode(self->result, arg->json, NULL);
		arg = self->result;
	}

	double value = 0;
	switch (arg->type) {
	case TYPE_DOUBLE:
		value = arg->dbl;
		break;
	case TYPE_BOOL:
	case TYPE_INT:
	case TYPE_TIMESTAMP:
		value = arg->integer;
		break;
	case TYPE_NULL:
		value_set_null(self->result);
		return;
	default:
		error("double(%s): operation type is not supported",
		      type_of(arg->type));
		break;
	}
	value_set_double(self->result, value);
}

hot static void
fn_json(Call* self)
{
	call_validate(self, 1);
	auto arg = self->argv[0];
	if (unlikely(arg.type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(arg.type == TYPE_JSON))
	{
		value_copy(self->result, &arg);
		return;
	}
	if (unlikely(arg.type != TYPE_STRING))
		error("json(%s): operation type is not supported",
		      type_of(arg.type));

	auto buf = buf_create();
	guard_buf(buf);
	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, &arg.string, buf);
	unguard();
	unguard();
	json_free(&json);
	value_set_json_buf(self->result, buf);
}

hot static void
fn_interval(Call* self)
{
	call_validate(self, 1);
	auto arg = self->argv[0];
	if (unlikely(arg.type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(arg.type == TYPE_INTERVAL))
	{
		value_copy(self->result, &arg);
		return;
	}
	if (unlikely(arg.type != TYPE_STRING))
		error("interval(%s): operation type is not supported",
		      type_of(arg.type));
	Interval iv;
	interval_init(&iv);
	interval_read(&iv, &arg.string);
	value_set_interval(self->result, &iv);
}

hot static void
fn_timestamp(Call* self)
{
	if (self->argc < 1 || self->argc > 2)
		error("timestamp(): unexpected number of arguments");
	// timestamp(string, timezone)
	// timestamp(string)
	// timestamp(int)
	switch (self->argv[0].type) {
	case TYPE_STRING:
	{
		Timezone* timezone = self->vm->local->timezone;
		if (self->argc == 2)
		{
			if (unlikely(self->argv[1].type == TYPE_NULL))
			{
				value_set_null(self->result);
				return;
			}
			call_validate_arg(self, 1, TYPE_STRING);
			auto name = &self->argv[1].string;
			timezone = timezone_mgr_find(global()->timezone_mgr, name);
			if (! timezone)
				error("timestamp(): failed to find timezone '%.*s'",
				      str_size(name), str_of(name));
		}
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read(&ts, &self->argv[0].string);
		auto time = timestamp_of(&ts, timezone);
		value_set_timestamp(self->result, time);
		break;
	}
	case TYPE_INT:
	{
		if (self->argc == 2)
			error("timestamp(): unexpected argument");
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read_value(&ts, self->argv[0].integer);
		value_set_timestamp(self->result, timestamp_of(&ts, NULL));
		break;
	}
	case TYPE_NULL:
	{
		value_set_null(self->result);
		break;
	}
	default:
		error("timestamp(%s): operation type is not supported",
		      type_of(self->argv[0].type));
	}
}

hot static void
fn_vector(Call* self)
{
	call_validate(self, 1);
	auto arg = self->argv[0];
	if (unlikely(arg.type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(arg.type == TYPE_VECTOR))
	{
		value_copy(self->result, &arg);
		return;
	}
	if (unlikely(arg.type != TYPE_JSON))
		error("vector(%s): operation type is not supported",
		      type_of(arg.type));

	uint8_t* pos = arg.json;
	if (! json_is_array(pos))
		error("vector(): json array expected");

	auto buf = buf_create();
	guard_buf(buf);
	buf_write_i32(buf, 0);

	json_read_array(&pos);
	int count = 0;
	while (! json_read_array_end(&pos))
	{
		float value_flt;
		if (json_is_real(pos))
		{
			double value;
			json_read_real(&pos, &value);
			value_flt = value;
		} else
		if (json_is_integer(pos))
		{
			int64_t value;
			json_read_integer(&pos, &value);
			value_flt = value;
		} else {
			error("vector(): json array int or real values expected");
		}
		buf_write_float(buf, value_flt);
		count++;
	}
	*buf_u32(buf) = count;

	unguard();
	value_set_vector_buf(self->result, buf);
}

FunctionDef fn_cast_def[] =
{
	{ "public", "type",      TYPE_STRING,    fn_type,      false },
	{ "public", "string",    TYPE_STRING,    fn_string,    false },
	{ "public", "int",       TYPE_INT,       fn_int,       false },
	{ "public", "bool",      TYPE_BOOL,      fn_bool,      false },
	{ "public", "double",    TYPE_DOUBLE,    fn_double,    false },
	{ "public", "json",      TYPE_JSON,      fn_json,      false },
	{ "public", "interval",  TYPE_INTERVAL,  fn_interval,  false },
	{ "public", "timestamp", TYPE_TIMESTAMP, fn_timestamp, false },
	{ "public", "vector",    TYPE_VECTOR,    fn_vector,    false },
	{  NULL,     NULL,       TYPE_NULL,      NULL,         false }
};
