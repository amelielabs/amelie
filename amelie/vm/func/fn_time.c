
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
fn_now(Call* self)
{
	call_expect(self, 0);
	value_set_timestamp(self->result, self->mgr->local->time_us);
}

hot static void
fn_at_timezone(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_expect_arg(self, 0, TYPE_TIMESTAMP);
	call_expect_arg(self, 1, TYPE_STRING);

	auto name = &self->argv[1].string;
	auto timezone = timezone_mgr_find(global()->timezone_mgr, name);
	if (! timezone)
		call_error_arg(self, 1, "failed to find timezone '%.*s'",
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
		call_error(self, "unexpected number of arguments");
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
		call_expect_arg(self, 1, TYPE_TIMESTAMP);
		iv = &argv[0].interval;
		timestamp = argv[1].integer;
	} else
	if (argv[0].type == TYPE_TIMESTAMP)
	{
		call_expect_arg(self, 1, TYPE_INTERVAL);
		timestamp = argv[0].integer;
		iv = &argv[1].interval;
	} else {
		call_unsupported(self, 0);
	}

	uint64_t origin;
	if (self->argc == 3)
	{
		call_expect_arg(self, 2, TYPE_TIMESTAMP);
		origin = argv[2].integer;
	} else
	{
		// default origin for 2001-01-01 00:00:00
		origin = 978307200000000ULL;
	}

	if (iv->m != 0)
		call_error(self, "month and year intervals are not supported");
	int64_t span = iv->d * 86400000000ULL + iv->us;
	if (span <= 0)
		call_error(self, "invalid interval");
	if (origin > timestamp)
		call_error(self, "origin is in the future");
	uint64_t at = timestamp - origin;
	uint64_t delta = at - at % span;
	value_set_timestamp(self->result, origin + delta);
}

hot static void
fn_date_trunc(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2 || self->argc > 3)
		call_error(self, "unexpected number of arguments");
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}

	// (string, timestamp [, timezone])
	// (timestamp, string [, timezone])
	Timezone* timezone = self->mgr->local->timezone;
	if (self->argc == 3)
	{
		call_expect_arg(self, 2, TYPE_STRING);
		auto name = &self->argv[2].string;
		timezone = timezone_mgr_find(global()->timezone_mgr, name);
		if (! timezone)
			call_error_arg(self, 2, "failed to find timezone '%.*s'",
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
			call_unsupported(self, 1);
		}
	} else
	if (argv[0].type == TYPE_TIMESTAMP)
	{
		call_expect_arg(self, 1, TYPE_STRING);
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_set_unixtime(&ts, self->argv[0].integer);
		timestamp_trunc(&ts, &argv[1].string);
		auto time = timestamp_get_unixtime(&ts, timezone);
		value_set_timestamp(self->result, time);
	} else
	{
		call_unsupported(self, 0);
	}
}

hot static void
fn_interval_trunc(Call* self)
{
	// (string, interval)
	// (interval, string)
	auto argv = self->argv;
	call_expect(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	if (argv[0].type == TYPE_STRING)
	{
		call_expect_arg(self, 2, TYPE_INTERVAL);
		Interval iv = argv[1].interval;
		interval_trunc(&iv, &argv[0].string);
		value_set_interval(self->result, &iv);
	} else
	if (argv[0].type == TYPE_INTERVAL)
	{
		call_expect_arg(self, 1, TYPE_STRING);
		Interval iv = argv[0].interval;
		interval_trunc(&iv, &argv[1].string);
		value_set_interval(self->result, &iv);
	} else
	{
		call_unsupported(self, 0);
	}
}

hot static void
fn_extract(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2 || self->argc > 3)
		call_error(self, "unexpected number of arguments");
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}

	// (string, interval)
	// (string, timestamp [, timezone])
	// (string, date)
	//
	// (interval, string)
	// (timestamp, string [, timezone])
	// (date, string)
	Timezone* timezone = self->mgr->local->timezone;
	if (self->argc == 3)
	{
		call_expect_arg(self, 2, TYPE_STRING);
		auto name = &self->argv[2].string;
		timezone = timezone_mgr_find(global()->timezone_mgr, name);
		if (! timezone)
			call_error_arg(self, 2, "failed to find timezone '%.*s'",
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
		if (argv[1].type == TYPE_DATE)
			value = date_extract(argv[1].integer, &argv[0].string);
		else
			call_unsupported(self, 1);
	} else
	if (argv[0].type == TYPE_INTERVAL)
	{
		call_expect_arg(self, 1, TYPE_STRING);
		value = interval_extract(&argv[0].interval, &argv[1].string);
	} else
	if (argv[0].type == TYPE_TIMESTAMP)
	{
		call_expect_arg(self, 1, TYPE_STRING);
		value = timestamp_extract(argv[0].integer, timezone, &argv[1].string);
	} else
	if (argv[0].type == TYPE_DATE)
	{
		call_expect_arg(self, 1, TYPE_STRING);
		value = date_extract(argv[0].integer, &argv[1].string);
	} else
	{
		call_unsupported(self, 0);
	}
	value_set_int(self->result, value);
}

#if 0
hot static void
fn_generate_series(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 3);
	call_expect_arg(self, 0, TYPE_TIMESTAMP);
	call_expect_arg(self, 1, TYPE_TIMESTAMP);
	call_expect_arg(self, 2, TYPE_INTERVAL);

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

void
fn_time_register(FunctionMgr* self)
{
	// public.now()
	Function* func;
	func = function_allocate(TYPE_TIMESTAMP, "public", "now", fn_now);
	function_mgr_add(self, func);

	// public.at_timezone()
	func = function_allocate(TYPE_STRING, "public", "at_timezone", fn_at_timezone);
	function_mgr_add(self, func);

	// public.date_bin()
	func = function_allocate(TYPE_TIMESTAMP, "public", "date_bin", fn_date_bin);
	function_mgr_add(self, func);

	// public.date_trunc()
	func = function_allocate(TYPE_TIMESTAMP, "public", "date_trunc", fn_date_trunc);
	function_mgr_add(self, func);

	// public.interval_trunc()
	func = function_allocate(TYPE_INTERVAL, "public", "interval_trunc", fn_interval_trunc);
	function_mgr_add(self, func);

	// public.extract()
	func = function_allocate(TYPE_INT, "public", "extract", fn_extract);
	function_mgr_add(self, func);
}
