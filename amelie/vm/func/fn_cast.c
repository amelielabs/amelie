
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
	call_validate(self);
	value_to_string(self->result, self->argv[0], self->vm->local->timezone);
}

hot static void
fn_int(Call* self)
{
	call_validate(self);
	value_to_int(self->result, self->argv[0]);
}

hot static void
fn_bool(Call* self)
{
	call_validate(self);
	value_to_bool(self->result, self->argv[0]);
}

hot static void
fn_real(Call* self)
{
	call_validate(self);
	value_to_real(self->result, self->argv[0]);
}

hot static void
fn_json(Call* self)
{
	call_validate(self);
	value_to_json(self->result, self->argv[0], self->vm->local->timezone);
}

hot static void
fn_interval(Call* self)
{
	call_validate(self);
	call_validate_arg(self, 0, VALUE_STRING);
	Interval iv;
	interval_init(&iv);
	interval_read(&iv, &self->argv[0]->string);
	value_set_interval(self->result, &iv);
}

hot static void
fn_timestamp(Call* self)
{
	call_validate(self);
	if (self->argv[0]->type == VALUE_STRING)
	{
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read(&ts, &self->argv[0]->string);
		auto time = timestamp_of(&ts, self->vm->local->timezone);
		value_set_timestamp(self->result, time);
	} else
	if (self->argv[0]->type == VALUE_INT)
	{
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read_value(&ts, self->argv[0]->integer);
		value_set_timestamp(self->result, timestamp_of(&ts, NULL));
	} else {
		error("timestamp(): exptected string or int");
	}
}

FunctionDef fn_cast[] =
{
	{ "public", "string",    fn_string,    1 },
	{ "public", "int",       fn_int,       1 },
	{ "public", "bool",      fn_bool,      1 },
	{ "public", "real",      fn_real,      1 },
	{ "public", "json",      fn_json,      1 },
	{ "public", "interval",  fn_interval,  1 },
	{ "public", "timestamp", fn_timestamp, 1 },
	{  NULL,     NULL,       NULL,         0 }
};
