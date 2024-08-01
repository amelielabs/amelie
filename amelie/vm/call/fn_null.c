
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
#include <amelie_call.h>

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
	call_validate(self);
	if (value_is_equal(self->argv[0], self->argv[1]))
	{
		value_set_null(self->result);
		return;
	}
	value_copy(self->result, self->argv[0]);
}

FunctionDef fn_null[] =
{
	{ "public", "coalesce", fn_coalesce, 0 },
	{ "public", "nullif",   fn_nullif,   2 },
	{  NULL,     NULL,      NULL,        0 }
};
