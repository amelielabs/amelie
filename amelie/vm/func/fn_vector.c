
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
#include <amelie_sync>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_commit.h>
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
	call_arg(self, 0, TYPE_VECTOR);
	call_arg(self, 1, TYPE_VECTOR);
	if (argv[0].vector_dim != argv[1].vector_dim)
		call_error(self, "vector sizes do not match");
	auto distance = vector_distance(argv[0].vector_dim, argv[0].vector, argv[1].vector);
	value_set_double(self->result, distance);
}

void
fn_vector_register(Functions* self)
{
	// cos_distance()
	Function* func;
	func = function_allocate(TYPE_DOUBLE, "cos_distance", fn_cos_distance);
	functions_add(self, func);
}
