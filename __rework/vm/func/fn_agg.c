
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
fn_agg(Call* self, int type)
{
	auto argv = self->argv;
	if (likely(self->argc == 2))
	{
		self->result->type = VALUE_AGG;
		value_agg(&self->result->agg, argv[0], type, argv[1]);
	} else
	if (self->argc == 1)
	{
		self->result->type = VALUE_AGG;
		Value state;
		value_init(&state);
		value_set_null(&state),
		value_agg(&self->result->agg, &state, type, argv[0]);
	} else {
		call_validate(self, 2);
	}
}

hot static void
fn_agg_count(Call* self)
{
	fn_agg(self, AGG_COUNT);
}

hot static void
fn_agg_min(Call* self)
{
	fn_agg(self, AGG_MIN);
}

hot static void
fn_agg_max(Call* self)
{
	fn_agg(self, AGG_MAX);
}

hot static void
fn_agg_sum(Call* self)
{
	fn_agg(self, AGG_SUM);
}

hot static void
fn_agg_avg(Call* self)
{
	fn_agg(self, AGG_AVG);
}

FunctionDef fn_agg_def[] =
{
	{ "public", "agg_count", fn_agg_count, false },
	{ "public", "agg_min",   fn_agg_min,   false },
	{ "public", "agg_max",   fn_agg_max,   false },
	{ "public", "agg_sum",   fn_agg_sum,   false },
	{ "public", "agg_avg",   fn_agg_avg,   false },
	{  NULL,     NULL,       NULL,         false }
};
