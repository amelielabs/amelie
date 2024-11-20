
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
	str_set_cstr(&string, value_type_to_string(arg.type));
	value_set_string(self->result, &string, NULL);
}

hot static void
fn_string(Call* self)
{
	call_validate(self, 1);
	auto arg = self->argv[0];
	if (unlikely(arg.type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	auto data = buf_create();
	switch (arg.type) {
	case VALUE_STRING:
		buf_printf(data, "%.*s", str_size(&arg.string), str_of(&arg.string));
		break;
	case VALUE_INTERVAL:
	{
		buf_reserve(data, 512);
		int size = interval_write(&arg.interval, (char*)data->position, 512);
		buf_advance(data, size);
		break;

	case VALUE_TIMESTAMP:
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
	if (arg->type == VALUE_JSON)
	{
		value_decode(self->result, arg->data, NULL);
		arg = self->result;
	}

	int64_t value = 0;
	switch (arg->type) {
	case VALUE_DOUBLE:
		value = arg->dbl;
		break;
	case VALUE_BOOL:
	case VALUE_INT:
	case VALUE_TIMESTAMP:
		value = arg->integer;
		break;
	case VALUE_STRING:
		if (str_toint(&arg->string, &value) == -1)
			error("int(): failed to cast string");
		break;
	case VALUE_NULL:
		value_set_null(self->result);
		return;
	default:
		error("int(%s): operation type is not supported",
		      value_type_to_string(arg->type));
		break;
	}
	value_set_int(self->result, value);
}

hot static void
fn_bool(Call* self)
{
	call_validate(self, 1);
	auto arg = &self->argv[0];
	if (arg->type == VALUE_JSON)
	{
		value_decode(self->result, arg->data, NULL);
		arg = self->result;
	}

	bool value = false;
	switch (arg->type) {
	case VALUE_DOUBLE:
		value = arg->dbl > 0.0;
		break;
	case VALUE_BOOL:
	case VALUE_INT:
	case VALUE_TIMESTAMP:
		value = arg->integer > 0;
		break;
	case VALUE_INTERVAL:
		value = (arg->interval.us + arg->interval.d + arg->interval.m) > 0;
		break;
	case VALUE_STRING:
		value = str_size(&arg->string) > 0;
		break;
	case VALUE_NULL:
		value_set_null(self->result);
		return;
	default:
		error("bool(%s): operation type is not supported",
		      value_type_to_string(arg->type));
		break;
	}
	value_set_bool(self->result, value);
}

hot static void
fn_double(Call* self)
{
	call_validate(self, 1);
	auto arg = &self->argv[0];
	if (arg->type == VALUE_JSON)
	{
		value_decode(self->result, arg->data, NULL);
		arg = self->result;
	}

	double value = 0;
	switch (arg->type) {
	case VALUE_DOUBLE:
		value = arg->dbl;
		break;
	case VALUE_BOOL:
	case VALUE_INT:
	case VALUE_TIMESTAMP:
		value = arg->integer;
		break;
	case VALUE_NULL:
		value_set_null(self->result);
		return;
	default:
		error("double(%s): operation type is not supported",
		      value_type_to_string(arg->type));
		break;
	}
	value_set_double(self->result, value);
}

hot static void
fn_json(Call* self)
{
	call_validate(self, 1);
	auto arg = self->argv[0];
	if (unlikely(arg.type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(arg.type == VALUE_JSON))
	{
		value_copy(self->result, &arg);
		return;
	}
	if (unlikely(arg.type != VALUE_STRING))
		error("json(%s): operation type is not supported",
		      value_type_to_string(arg.type));

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
	if (unlikely(arg.type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(arg.type == VALUE_INTERVAL))
	{
		value_copy(self->result, &arg);
		return;
	}
	if (unlikely(arg.type != VALUE_STRING))
		error("interval(%s): operation type is not supported",
		      value_type_to_string(arg.type));
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
	case VALUE_STRING:
	{
		Timezone* timezone = self->vm->local->timezone;
		if (self->argc == 2)
		{
			if (unlikely(self->argv[1].type == VALUE_NULL))
			{
				value_set_null(self->result);
				return;
			}
			call_validate_arg(self, 1, VALUE_STRING);
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
	case VALUE_INT:
	{
		if (self->argc == 2)
			error("timestamp(): unexpected argument");
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read_value(&ts, self->argv[0].integer);
		value_set_timestamp(self->result, timestamp_of(&ts, NULL));
		break;
	}
	case VALUE_NULL:
	{
		value_set_null(self->result);
		break;
	}
	default:
		error("timestamp(%s): operation type is not supported",
		      value_type_to_string(self->argv[0].type));
	}
}

hot static void
fn_vector(Call* self)
{
	call_validate(self, 1);
	auto arg = self->argv[0];
	if (unlikely(arg.type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(arg.type == VALUE_VECTOR))
	{
		value_copy(self->result, &arg);
		return;
	}
	if (unlikely(arg.type != VALUE_JSON))
		error("vector(%s): operation type is not supported",
		      value_type_to_string(arg.type));

	uint8_t* pos = arg.data;
	if (! data_is_array(pos))
		error("vector(): json array expected");

	auto buf = buf_create();
	guard_buf(buf);
	buf_write_i32(buf, 0);

	data_read_array(&pos);
	int count = 0;
	while (! data_read_array_end(&pos))
	{
		float value_flt;
		if (data_is_real(pos))
		{
			double value;
			data_read_real(&pos, &value);
			value_flt = value;
		} else
		if (data_is_integer(pos))
		{
			int64_t value;
			data_read_integer(&pos, &value);
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
	{ "public", "type",      VALUE_STRING,    fn_type,      false },
	{ "public", "string",    VALUE_STRING,    fn_string,    false },
	{ "public", "int",       VALUE_INT,       fn_int,       false },
	{ "public", "bool",      VALUE_BOOL,      fn_bool,      false },
	{ "public", "double",    VALUE_DOUBLE,    fn_double,    false },
	{ "public", "json",      VALUE_JSON,      fn_json,      false },
	{ "public", "interval",  VALUE_INTERVAL,  fn_interval,  false },
	{ "public", "timestamp", VALUE_TIMESTAMP, fn_timestamp, false },
	{ "public", "vector",    VALUE_VECTOR,    fn_vector,    false },
	{  NULL,     NULL,       VALUE_NONE,      NULL,         false }
};
