
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
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_now(Call* self)
{
	call_validate(self, 0);
	value_set_timestamp(self->result, self->vm->local->time_us);
}

hot static void
fn_at_timezone(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_validate_arg(self, 0, TYPE_TIMESTAMP);
	call_validate_arg(self, 1, TYPE_STRING);

	auto name = &self->argv[1].string;
	auto timezone = timezone_mgr_find(global()->timezone_mgr, name);
	if (! timezone)
		error("timestamp(): failed to find timezone '%.*s'",
		      str_size(name), str_of(name));

	auto data = buf_create();
	buf_reserve(data, 128);
	int size = timestamp_get(argv[0].integer, timezone, (char*)data->position, 128);
	buf_advance(data, size);

	Str string;
	str_init(&string);
	str_set(&string, (char*)data->start, buf_size(data));
	value_set_string(self->result, &string, data);
}

hot static void
fn_date_bin(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2 || self->argc > 3)
		error("date_bin(): unexpected number of arguments");
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}

	// (interval, timestamp [, timestamp_origin])
	// (timestamp, interval [, timestamp_origin])
	Interval* iv;
	uint64_t  timestamp;
	if (argv[0].type == TYPE_INTERVAL)
	{
		call_validate_arg(self, 1, TYPE_TIMESTAMP);
		iv = &argv[0].interval;
		timestamp = argv[1].integer;
	} else
	if (argv[0].type == TYPE_TIMESTAMP)
	{
		call_validate_arg(self, 1, TYPE_INTERVAL);
		timestamp = argv[0].integer;
		iv = &argv[1].interval;
	} else {
		error("date_bin(): invalid arguments");
	}

	uint64_t origin;
	if (self->argc == 3)
	{
		call_validate_arg(self, 2, TYPE_TIMESTAMP);
		origin = argv[2].integer;
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
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}

	// (string, timestamp [, timezone])
	// (timestamp, string [, timezone])
	Timezone* timezone = self->vm->local->timezone;
	if (self->argc == 3)
	{
		call_validate_arg(self, 2, TYPE_STRING);
		auto name = &self->argv[2].string;
		timezone = timezone_mgr_find(global()->timezone_mgr, name);
		if (! timezone)
			error("date_trunc(): failed to find timezone '%.*s'",
			      str_size(name), str_of(name));
	}

	if (argv[0].type == TYPE_STRING)
	{
		if (argv[1].type == TYPE_TIMESTAMP)
		{
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_set_unixtime(&ts, self->argv[1].integer);
			timestamp_trunc(&ts, &argv[0].string);
			auto time = timestamp_get_unixtime(&ts, timezone);
			value_set_timestamp(self->result, time);
		} else {
			error("date_trunc(): invalid arguments");
		}
	} else
	if (argv[0].type == TYPE_TIMESTAMP)
	{
		call_validate_arg(self, 1, TYPE_STRING);
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_set_unixtime(&ts, self->argv[0].integer);
		timestamp_trunc(&ts, &argv[1].string);
		auto time = timestamp_get_unixtime(&ts, timezone);
		value_set_timestamp(self->result, time);
	} else
	{
		error("date_trunc(): invalid arguments");
	}
}

hot static void
fn_interval_trunc(Call* self)
{
	// (string, interval)
	// (interval, string)
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (argv[0].type == TYPE_STRING)
	{
		call_validate_arg(self, 2, TYPE_INTERVAL);
		Interval iv = argv[1].interval;
		interval_trunc(&iv, &argv[0].string);
		value_set_interval(self->result, &iv);
	} else
	if (argv[0].type == TYPE_INTERVAL)
	{
		call_validate_arg(self, 1, TYPE_STRING);
		Interval iv = argv[0].interval;
		interval_trunc(&iv, &argv[1].string);
		value_set_interval(self->result, &iv);
	} else
	{
		error("interval_trunc(): invalid arguments");
	}
}

hot static void
fn_extract(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2 || self->argc > 3)
		error("extract(): unexpected number of arguments");
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}

	// (string, interval)
	// (string, timestamp [, timezone])
	//
	// (interval, string)
	// (timestamp, string [, timezone])
	Timezone* timezone = self->vm->local->timezone;
	if (self->argc == 3)
	{
		call_validate_arg(self, 2, TYPE_STRING);
		auto name = &self->argv[2].string;
		timezone = timezone_mgr_find(global()->timezone_mgr, name);
		if (! timezone)
			error("extract(): failed to find timezone '%.*s'",
			      str_size(name), str_of(name));
	}

	uint64_t value;
	if (argv[0].type == TYPE_STRING)
	{
		if (argv[1].type == TYPE_INTERVAL)
			value = interval_extract(&argv[1].interval, &argv[0].string);
		else
		if (argv[1].type == TYPE_TIMESTAMP)
			value = timestamp_extract(argv[1].integer, timezone, &argv[0].string);
		else
			error("extract(): invalid arguments");
	} else
	if (argv[0].type == TYPE_INTERVAL)
	{
		call_validate_arg(self, 1, TYPE_STRING);
		value = interval_extract(&argv[0].interval, &argv[1].string);
	} else
	if (argv[0].type == TYPE_TIMESTAMP)
	{
		call_validate_arg(self, 1, TYPE_STRING);
		value = timestamp_extract(argv[0].integer, timezone, &argv[1].string);
	} else
	{
		error("extract(): invalid arguments");
	}
	value_set_int(self->result, value);
}

#if 0
hot static void
fn_generate_series(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 3);
	call_validate_arg(self, 0, TYPE_TIMESTAMP);
	call_validate_arg(self, 1, TYPE_TIMESTAMP);
	call_validate_arg(self, 2, TYPE_INTERVAL);

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

#endif

FunctionDef fn_time_def[] =
{
	{ "public", "now",             TYPE_TIMESTAMP, fn_now,             FN_NONE },
	{ "public", "at_timezone",     TYPE_STRING,    fn_at_timezone,     FN_NONE },
	{ "public", "date_bin",        TYPE_TIMESTAMP, fn_date_bin,        FN_NONE },
	{ "public", "date_trunc",      TYPE_TIMESTAMP, fn_date_trunc,      FN_NONE },
	{ "public", "interval_trunc",  TYPE_INTERVAL,  fn_interval_trunc,  FN_NONE },
	{ "public", "extract",         TYPE_INT,       fn_extract,         FN_NONE },
	{  NULL,     NULL,             TYPE_NULL,      NULL,               FN_NONE }
};
