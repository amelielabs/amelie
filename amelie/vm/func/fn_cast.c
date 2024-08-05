
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
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_string(Call* self)
{
	call_validate(self, 1);
	value_to_string(self->result, self->argv[0], self->vm->local->timezone);
}

hot static void
fn_int(Call* self)
{
	call_validate(self, 1);
	value_to_int(self->result, self->argv[0]);
}

hot static void
fn_bool(Call* self)
{
	call_validate(self, 1);
	value_to_bool(self->result, self->argv[0]);
}

hot static void
fn_real(Call* self)
{
	call_validate(self, 1);
	value_to_real(self->result, self->argv[0]);
}

hot static void
fn_json(Call* self)
{
	call_validate(self, 1);
	value_to_json(self->result, self->argv[0], self->vm->local->timezone);
}

hot static void
fn_interval(Call* self)
{
	call_validate(self, 1);
	call_validate_arg(self, 0, VALUE_STRING);
	Interval iv;
	interval_init(&iv);
	interval_read(&iv, &self->argv[0]->string);
	value_set_interval(self->result, &iv);
}

hot static void
fn_timestamp(Call* self)
{
	if (self->argc < 1 || self->argc > 2)
		error("timestamp(): unexpected number of arguments");
	if (self->argv[0]->type == VALUE_STRING)
	{
		Timezone* timezone = self->vm->local->timezone;
		if (self->argc == 2)
		{
			call_validate_arg(self, 1, VALUE_STRING);
			auto name = &self->argv[1]->string;
			timezone = timezone_mgr_find(global()->timezone_mgr, name);
			if (! timezone)
				error("timestamp(): failed to find timezone '%.*s'",
				      str_size(name), str_of(name));
		}
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read(&ts, &self->argv[0]->string);
		auto time = timestamp_of(&ts, timezone);
		value_set_timestamp(self->result, time);
	} else
	if (self->argv[0]->type == VALUE_INT)
	{
		if (self->argc == 2)
			error("timestamp(): unexpected argument");
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read_value(&ts, self->argv[0]->integer);
		value_set_timestamp(self->result, timestamp_of(&ts, NULL));
	} else {
		error("timestamp(): exptected string or int");
	}
}

FunctionDef fn_cast_def[] =
{
	{ "public", "string",    fn_string,    false },
	{ "public", "int",       fn_int,       false },
	{ "public", "bool",      fn_bool,      false },
	{ "public", "real",      fn_real,      false },
	{ "public", "json",      fn_json,      false },
	{ "public", "interval",  fn_interval,  false },
	{ "public", "timestamp", fn_timestamp, false },
	{  NULL,     NULL,       NULL,         false }
};
