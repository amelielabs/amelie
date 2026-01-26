
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>

hot static void
fn_cos_distance(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	fn_expect_arg(self, 0, TYPE_VECTOR);
	fn_expect_arg(self, 1, TYPE_VECTOR);
	if (argv[0].vector->size != argv[1].vector->size)
		fn_error(self, "vector sizes do not match");
	auto distance = vector_distance(argv[0].vector, argv[1].vector);
	value_set_double(self->result, distance);
}

void
fn_vector_register(FunctionMgr* self)
{
	// cos_distance()
	Function* func;
	func = function_allocate(TYPE_DOUBLE, "cos_distance", fn_cos_distance);
	function_mgr_add(self, func);
}
