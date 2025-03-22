
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
fn_type(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	Str string;
	str_set_cstr(&string, type_of(arg->type));
	value_set_string(self->result, &string, NULL);
}

hot static void
fn_int(Call* self)
{
	call_expect(self, 1);
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
	case TYPE_DATE:
		value = arg->integer;
		break;
	case TYPE_STRING:
		if (str_toint(&arg->string, &value) == -1)
			call_error_arg(self, 0, "failed to cast string");
		break;
	case TYPE_NULL:
		value_set_null(self->result);
		return;
	default:
		call_unsupported(self, 0);
		break;
	}
	value_set_int(self->result, value);
}

hot static void
fn_bool(Call* self)
{
	call_expect(self, 1);
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
		value = arg->integer > 0;
		break;
	case TYPE_INTERVAL:
		value = (arg->interval.us + arg->interval.d + arg->interval.m) > 0;
		break;
	case TYPE_STRING:
		if (str_is_case(&arg->string, "true", 4))
			value = true;
		else
		if (str_is_case(&arg->string, "false", 5))
			value = false;
		else
			call_error_arg(self, 0, "failed to cast string to bool");
		break;
	case TYPE_NULL:
		value_set_null(self->result);
		return;
	default:
		call_unsupported(self, 0);
		break;
	}
	value_set_bool(self->result, value);
}

hot static void
fn_double(Call* self)
{
	call_expect(self, 1);
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
	case TYPE_DATE:
		value = arg->integer;
		break;
	case TYPE_NULL:
		value_set_null(self->result);
		return;
	default:
		call_unsupported(self, 0);
		break;
	}
	value_set_double(self->result, value);
}

hot static void
fn_string(Call* self)
{
	call_expect(self, 1);
	auto arg = &self->argv[0];
	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	auto data = buf_create();
	switch (arg->type) {
	case TYPE_STRING:
		buf_printf(data, "%.*s", str_size(&arg->string), str_of(&arg->string));
		break;
	case TYPE_DATE:
	{
		buf_reserve(data, 512);
		int size = date_get(arg->integer, (char*)data->position, 512);
		buf_advance(data, size);
		break;
	}
	case TYPE_INTERVAL:
	{
		buf_reserve(data, 512);
		int size = interval_get(&arg->interval, (char*)data->position, 512);
		buf_advance(data, size);
		break;
	}
	case TYPE_TIMESTAMP:
	{
		buf_reserve(data, 128);
		int size = timestamp_get(arg->integer, self->mgr->local->timezone, (char*)data->position, 128);
		buf_advance(data, size);
		break;
	}
	case TYPE_UUID:
	{
		buf_reserve(data, UUID_SZ);
		uuid_get(&arg->uuid, (char*)data->position, UUID_SZ);
		int size = UUID_SZ - 1;
		buf_advance(data, size);
		break;
	}
	default:
		// json string without quotes
		if (arg->type == TYPE_JSON && json_is_string(arg->json))
		{
			auto pos = arg->json;
			Str str;
			json_read_string(&pos, &str);
			buf_write_str(data, &str);
			break;
		}
		value_export(arg, self->mgr->local->timezone, false, data);
		break;
	}
	Str string;
	str_init(&string);
	str_set(&string, (char*)data->start, buf_size(data));
	value_set_string(self->result, &string, data);
}

hot static void
fn_json(Call* self)
{
	call_expect(self, 1);
	auto arg = &self->argv[0];
	if (unlikely(arg->type == TYPE_JSON))
	{
		value_copy(self->result, arg);
		return;
	}
	auto buf = buf_create();
	value_encode(arg, self->mgr->local->timezone, buf);
	value_set_json_buf(self->result, buf);
}

hot static void
fn_json_import(Call* self)
{
	call_expect(self, 1);
	auto arg = &self->argv[0];
	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(arg->type == TYPE_JSON))
	{
		value_copy(self->result, arg);
		return;
	}
	if (unlikely(arg->type != TYPE_STRING))
		call_unsupported(self, 0);

	auto buf = buf_create();
	errdefer_buf(buf);

	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &arg->string, buf);

	value_set_json_buf(self->result, buf);
}

hot static void
fn_interval(Call* self)
{
	call_expect(self, 1);
	auto arg = &self->argv[0];
	if (arg->type == TYPE_JSON)
	{
		value_decode(self->result, arg->json, NULL);
		arg = self->result;
	}

	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(arg->type == TYPE_INTERVAL))
	{
		value_copy(self->result, arg);
		return;
	}
	if (unlikely(arg->type != TYPE_STRING))
		call_unsupported(self, 0);
	Interval iv;
	interval_init(&iv);
	interval_set(&iv, &arg->string);
	value_set_interval(self->result, &iv);
}

hot static void
fn_timestamp(Call* self)
{
	if (self->argc < 1 || self->argc > 2)
		call_error(self, "unexpected number of arguments");
	auto arg = &self->argv[0];
	if (arg->type == TYPE_JSON)
	{
		value_decode(self->result, arg->json, NULL);
		arg = self->result;
	}
	// timestamp(string, timezone)
	// timestamp(string)
	// timestamp(int)
	// timestamp(date)
	switch (arg->type) {
	case TYPE_STRING:
	{
		Timezone* timezone = self->mgr->local->timezone;
		if (self->argc == 2)
		{
			if (unlikely(self->argv[1].type == TYPE_NULL))
			{
				value_set_null(self->result);
				return;
			}
			call_expect_arg(self, 1, TYPE_STRING);
			auto name = &self->argv[1].string;
			timezone = timezone_mgr_find(global()->timezone_mgr, name);
			if (! timezone)
				call_error_arg(self, 1, "failed to find timezone '%.*s'",
				               str_size(name), str_of(name));
		}
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_set(&ts, &arg->string);
		auto time = timestamp_get_unixtime(&ts, timezone);
		value_set_timestamp(self->result, time);
		break;
	}
	case TYPE_INT:
	{
		if (self->argc == 2)
			call_error_arg(self, 1, "unexpected argument");
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_set_unixtime(&ts, arg->integer);
		value_set_timestamp(self->result, timestamp_get_unixtime(&ts, NULL));
		break;
	}
	case TYPE_TIMESTAMP:
	{
		value_set_timestamp(self->result, arg->integer);
		break;
	}
	case TYPE_DATE:
	{
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_set_date(&ts, arg->integer);
		value_set_timestamp(self->result, timestamp_get_unixtime(&ts, NULL));
		break;
	}
	case TYPE_NULL:
	{
		value_set_null(self->result);
		break;
	}
	default:
		call_unsupported(self, 0);
	}
}

hot static void
fn_date(Call* self)
{
	call_expect(self, 1);
	auto arg = &self->argv[0];
	if (arg->type == TYPE_JSON)
	{
		value_decode(self->result, arg->json, NULL);
		arg = self->result;
	}

	// date(string)
	// date(timestamp)
	// date(int)
	switch (arg->type) {
	case TYPE_STRING:
	{
		auto julian = date_set(&arg->string);
		value_set_date(self->result, julian);
		break;
	}
	case TYPE_DATE:
	{
		value_set_date(self->result, arg->integer);
		break;
	}
	case TYPE_TIMESTAMP:
	{
		value_set_date(self->result, timestamp_date(arg->integer));
		break;
	}
	case TYPE_INT:
	{
		auto julian = date_set_julian(arg->integer);
		value_set_date(self->result, julian);
		break;
	}
	case TYPE_NULL:
	{
		value_set_null(self->result);
		break;
	}
	default:
		call_unsupported(self, 0);
	}
}

hot static void
fn_vector(Call* self)
{
	call_expect(self, 1);
	auto arg = &self->argv[0];
	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(arg->type == TYPE_VECTOR))
	{
		value_copy(self->result, arg);
		return;
	}
	if (unlikely(arg->type != TYPE_JSON))
		call_unsupported(self, 0);

	uint8_t* pos = arg->json;
	if (! json_is_array(pos))
		call_error_arg(self, 0, "json array expected");

	auto buf = buf_create();
	errdefer_buf(buf);
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
			call_error_arg(self, 0, "json array values must be int or float");
		}
		buf_write_float(buf, value_flt);
		count++;
	}
	*buf_u32(buf) = count;

	value_set_vector_buf(self->result, buf);
}

hot static void
fn_uuid(Call* self)
{
	call_expect(self, 1);
	auto arg = &self->argv[0];
	if (arg->type == TYPE_JSON)
	{
		value_decode(self->result, arg->json, NULL);
		arg = self->result;
	}

	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (unlikely(arg->type == TYPE_UUID))
	{
		value_copy(self->result, arg);
		return;
	}
	if (unlikely(arg->type != TYPE_STRING))
		call_unsupported(self, 0);
	Uuid uuid;
	uuid_init(&uuid);
	uuid_set(&uuid, &arg->string);
	value_set_uuid(self->result, &uuid);
}

void
fn_cast_register(FunctionMgr* self)
{
	// public.type()
	Function* func;
	func = function_allocate(TYPE_STRING, "public", "type", fn_type);
	function_mgr_add(self, func);

	// public.int()
	func = function_allocate(TYPE_INT, "public", "int", fn_int);
	function_mgr_add(self, func);

	// public.bool()
	func = function_allocate(TYPE_BOOL, "public", "bool", fn_bool);
	function_mgr_add(self, func);

	// public.double()
	func = function_allocate(TYPE_DOUBLE, "public", "double", fn_double);
	function_mgr_add(self, func);

	// public.string()
	func = function_allocate(TYPE_STRING, "public", "string", fn_string);
	function_mgr_add(self, func);

	// public.json()
	func = function_allocate(TYPE_JSON, "public", "json", fn_json);
	function_mgr_add(self, func);

	// public.json_import()
	func = function_allocate(TYPE_JSON, "public", "json_import", fn_json_import);
	function_mgr_add(self, func);

	// public.interval()
	func = function_allocate(TYPE_INTERVAL, "public", "interval", fn_interval);
	function_mgr_add(self, func);

	// public.timestamp()
	func = function_allocate(TYPE_TIMESTAMP, "public", "timestamp", fn_timestamp);
	function_mgr_add(self, func);

	// public.date()
	func = function_allocate(TYPE_DATE, "public", "date", fn_date);
	function_mgr_add(self, func);

	// public.vector()
	func = function_allocate(TYPE_VECTOR, "public", "vector", fn_vector);
	function_mgr_add(self, func);

	// public.uuid()
	func = function_allocate(TYPE_UUID, "public", "uuid", fn_uuid);
	function_mgr_add(self, func);
}
