
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
fn_cos_distance(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_expect_arg(self, 0, TYPE_VECTOR);
	call_expect_arg(self, 1, TYPE_VECTOR);
	if (argv[0].vector->size != argv[1].vector->size)
		call_error(self, "vector sizes do not match");
	auto distance = vector_distance(argv[0].vector, argv[1].vector);
	value_set_double(self->result, distance);
}

void
fn_vector_register(FunctionMgr* self)
{
	// public.cos_distance()
	Function* func;
	func = function_allocate(TYPE_DOUBLE, "public", "cos_distance", fn_cos_distance);
	function_mgr_add(self, func);
}
