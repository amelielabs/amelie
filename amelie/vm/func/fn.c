
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
fn_sizeof(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	if (arg->type == VALUE_SET)
		value_set_int(self->result, ((Set*)arg->obj)->list_count);
	else
		value_sizeof(self->result, arg);
}

hot static void
fn_generate_series(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
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

	auto buf = buf_begin();
	encode_array(buf);
	while (pos <= end)
	{
		encode_timestamp(buf, pos);
		timestamp_add(&ts, &argv[2]->interval);
		pos = timestamp_of(&ts, NULL);
	}
	encode_array_end(buf);
	buf_end(buf);
	value_set_buf(self->result, buf);
}

hot static void
fn_time_bucket(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	call_validate_arg(self, 0, VALUE_INTERVAL);
	call_validate_arg(self, 1, VALUE_TIMESTAMP);
	auto iv = &argv[0]->interval;
	if (iv->m != 0)
		error("time_bucket(): month intervals are not supported");
	uint64_t span = iv->us + iv->d * 86400000000ULL;
	uint64_t ts = argv[1]->integer / span * span;
	value_set_timestamp(self->result, ts);
}

hot static void
fn_now(Call* self)
{
	call_validate(self);
	value_set_timestamp(self->result, time_us());
}

hot static void
fn_error(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_STRING);
	error("%.*s", str_size(&arg->string), str_of(&arg->string));
}

void
fn_register(FunctionMgr* mgr)
{
	FunctionDef def[] =
	{
		// string
		{ "public", "sizeof",          fn_sizeof,          1 },

		// time
		{ "public", "generate_series", fn_generate_series, 3 },
		{ "public", "time_bucket",     fn_time_bucket,     2 },
		{ "public", "now",             fn_now,             0 },

		// misc
		{ "public", "error",           fn_error,           1 },

		{  NULL,     NULL,             NULL,               0 }
	};
	function_mgr_register(mgr, fn_system);
	function_mgr_register(mgr, fn_cast);
	function_mgr_register(mgr, fn_null);
	function_mgr_register(mgr, fn_object);
	function_mgr_register(mgr, def);
}
