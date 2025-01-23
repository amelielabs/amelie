
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
fn_coalesce(Call* self)
{
	for (int i = 0; i < self->argc; i++)
	{
		if (self->argv[i].type != TYPE_NULL)
		{
			value_copy(self->result, &self->argv[i]);
			return;
		}
	}
	value_set_null(self->result);
}

hot static void
fn_nullif(Call* self)
{
	call_expect(self, 2);
	if (! value_compare(&self->argv[0], &self->argv[1]))
	{
		value_set_null(self->result);
		return;
	}
	value_copy(self->result, &self->argv[0]);
}

FunctionDef fn_null_def[] =
{
	{ "public", "coalesce", TYPE_NULL, fn_coalesce, FN_TYPE_DERIVE },
	{ "public", "nullif",   TYPE_NULL, fn_nullif,   FN_TYPE_DERIVE },
	{  NULL,     NULL,      TYPE_NULL, NULL,        FN_NONE        }
};
