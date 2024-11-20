
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
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_cos_distance(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == VALUE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_validate_arg(self, 0, VALUE_VECTOR);
	call_validate_arg(self, 1, VALUE_VECTOR);
	if (argv[0].vector->size != argv[1].vector->size)
		error("cos_distance(): vector sizes does not match");
	auto distance = vector_distance(argv[0].vector, argv[1].vector);
	value_set_double(self->result, distance);
}

FunctionDef fn_vector_def[] =
{
	{ "public", "cos_distance", VALUE_DOUBLE, fn_cos_distance, false },
	{  NULL,     NULL,          VALUE_NONE,   NULL,            false }
};
