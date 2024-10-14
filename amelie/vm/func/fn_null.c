
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_coalesce(Call* self)
{
	for (int i = 0; i < self->argc; i++)
	{
		if (self->argv[i]->type != VALUE_NULL)
		{
			value_copy(self->result, self->argv[i]);
			return;
		}
	}
	value_set_null(self->result);
}

hot static void
fn_nullif(Call* self)
{
	call_validate(self, 2);
	if (value_is_equal(self->argv[0], self->argv[1]))
	{
		value_set_null(self->result);
		return;
	}
	value_copy(self->result, self->argv[0]);
}

FunctionDef fn_null_def[] =
{
	{ "public", "coalesce", fn_coalesce, false },
	{ "public", "nullif",   fn_nullif,   false },
	{  NULL,     NULL,      NULL,        false }
};
