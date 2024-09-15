
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_now(Call* self)
{
	call_validate(self, 0);
	value_set_timestamp(self->result, time_us());
}

hot static void
fn_at_timezone(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, VALUE_TIMESTAMP);
	call_validate_arg(self, 1, VALUE_STRING);

	auto name = &self->argv[1]->string;
	auto timezone = timezone_mgr_find(global()->timezone_mgr, name);
	if (! timezone)
		error("timestamp(): failed to find timezone '%.*s'",
		      str_size(name), str_of(name));

	auto data = buf_create();
	buf_reserve(data, 128);
	int size = timestamp_write(argv[0]->integer, timezone,
	                           (char*)data->position, 128);
	buf_advance(data, size);

	Str string;
	str_init(&string);
	str_set(&string, (char*)data->start, buf_size(data));
	value_set_string(self->result, &string, data);
}

hot static void
fn_generate_series(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 3);
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

	auto buf = buf_create();
	encode_array(buf);
	while (pos <= end)
	{
		encode_timestamp(buf, pos);
		timestamp_add(&ts, &argv[2]->interval);
		pos = timestamp_of(&ts, NULL);
	}
	encode_array_end(buf);
	value_set_array_buf(self->result, buf);
}

hot static void
fn_date_bin(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2 || self->argc > 3)
		error("date_bin(): unexpected number of arguments");

	// (interval, timestamp [, timestamp_origin])
	// (timestamp, interval [, timestamp_origin])
	Interval* iv;
	uint64_t  timestamp;
	if (argv[0]->type == VALUE_INTERVAL)
	{
		call_validate_arg(self, 1, VALUE_TIMESTAMP);
		iv = &argv[0]->interval;
		timestamp = argv[1]->integer;
	} else
	if (argv[0]->type == VALUE_TIMESTAMP)
	{
		call_validate_arg(self, 1, VALUE_INTERVAL);
		timestamp = argv[0]->integer;
		iv = &argv[1]->interval;
	} else {
		error("date_bin(): invalid arguments");
	}

	uint64_t origin;
	if (self->argc == 3)
	{
		call_validate_arg(self, 2, VALUE_TIMESTAMP);
		origin = argv[2]->integer;
	} else
	{
		// default origin for 2001-01-01 00:00:00
		origin = 978307200000000ULL;
	}

	if (iv->m != 0)
		error("date_bin(): month and year intervals are not supported");
	int64_t span = iv->d * 86400000000ULL + iv->us;
	if (span <= 0)
		error("date_bin(): invalid argument");
	if (origin > timestamp)
		error("date_bin(): origin is in the future");
	uint64_t at = timestamp - origin;
	uint64_t delta = at - at % span;
	value_set_timestamp(self->result, origin + delta);
}

hot static void
fn_date_trunc(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2 || self->argc > 3)
		error("date_trunc(): unexpected number of arguments");

	// (string, interval)
	// (string, timestamp [, timezone])
	//
	// (interval, string)
	// (timestamp, string [, timezone])
	Timezone* timezone = self->vm->local->timezone;
	if (self->argc == 3)
	{
		call_validate_arg(self, 2, VALUE_STRING);
		auto name = &self->argv[2]->string;
		timezone = timezone_mgr_find(global()->timezone_mgr, name);
		if (! timezone)
			error("date_trunc(): failed to find timezone '%.*s'",
			      str_size(name), str_of(name));
	}

	if (argv[0]->type == VALUE_STRING)
	{
		if (argv[1]->type == VALUE_INTERVAL)
		{
			Interval iv = argv[1]->interval;
			interval_trunc(&iv, &argv[0]->string);
			value_set_interval(self->result, &iv);
		} else
		if (argv[1]->type == VALUE_TIMESTAMP)
		{
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read_value(&ts, self->argv[1]->integer);
			timestamp_trunc(&ts, &argv[0]->string);
			auto time = timestamp_of(&ts, timezone);
			value_set_timestamp(self->result, time);
		} else {
			error("date_trunc(): invalid arguments");
		}
	} else
	if (argv[0]->type == VALUE_INTERVAL)
	{
		call_validate_arg(self, 1, VALUE_STRING);
		Interval iv = argv[0]->interval;
		interval_trunc(&iv, &argv[1]->string);
		value_set_interval(self->result, &iv);
	} else
	if (argv[0]->type == VALUE_TIMESTAMP)
	{
		call_validate_arg(self, 1, VALUE_STRING);
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read_value(&ts, self->argv[0]->integer);
		timestamp_trunc(&ts, &argv[1]->string);
		auto time = timestamp_of(&ts, timezone);
		value_set_timestamp(self->result, time);
	} else
	{
		error("date_trunc(): invalid arguments");
	}
}

hot static void
fn_extract(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2 || self->argc > 3)
		error("extract(): unexpected number of arguments");

	// (string, interval)
	// (string, timestamp [, timezone])
	//
	// (interval, string)
	// (timestamp, string [, timezone])
	Timezone* timezone = self->vm->local->timezone;
	if (self->argc == 3)
	{
		call_validate_arg(self, 2, VALUE_STRING);
		auto name = &self->argv[2]->string;
		timezone = timezone_mgr_find(global()->timezone_mgr, name);
		if (! timezone)
			error("extract(): failed to find timezone '%.*s'",
			      str_size(name), str_of(name));
	}

	uint64_t value;
	if (argv[0]->type == VALUE_STRING)
	{
		if (argv[1]->type == VALUE_INTERVAL)
			value = interval_extract(&argv[1]->interval, &argv[0]->string);
		else
		if (argv[1]->type == VALUE_TIMESTAMP)
			value = timestamp_extract(argv[1]->integer, timezone, &argv[0]->string);
		else
			error("extract(): invalid arguments");
	} else
	if (argv[0]->type == VALUE_INTERVAL)
	{
		call_validate_arg(self, 1, VALUE_STRING);
		value = interval_extract(&argv[0]->interval, &argv[1]->string);
	} else
	if (argv[0]->type == VALUE_TIMESTAMP)
	{
		call_validate_arg(self, 1, VALUE_STRING);
		value = timestamp_extract(argv[0]->integer, timezone, &argv[1]->string);
	} else
	{
		error("extract(): invalid arguments");
	}
	value_set_int(self->result, value);
}

FunctionDef fn_time_def[] =
{
	{ "public", "now",             fn_now,             false },
	{ "public", "at_timezone",     fn_at_timezone,     false },
	{ "public", "generate_series", fn_generate_series, false },
	{ "public", "date_bin",        fn_date_bin,        false },
	{ "public", "date_trunc",      fn_date_trunc,      false },
	{ "public", "extract",         fn_extract,         false },
	{  NULL,     NULL,             NULL,               false }
};
