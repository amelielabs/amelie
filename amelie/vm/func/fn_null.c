
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
#include <amelie_executor.h>
#include <amelie_func.h>

hot static void
fn_coalesce(Fn* self)
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
fn_nullif(Fn* self)
{
	fn_expect(self, 2);
	if (! value_compare(&self->argv[0], &self->argv[1]))
	{
		value_set_null(self->result);
		return;
	}
	value_copy(self->result, &self->argv[0]);
}

void
fn_null_register(FunctionMgr* self)
{
	// coalesce()
	Function* func;
	func = function_allocate(TYPE_NULL, "coalesce", fn_coalesce);
	function_unset(func, FN_CONST);
	function_set(func, FN_DERIVE);
	function_mgr_add(self, func);

	// nullif()
	func = function_allocate(TYPE_NULL, "nullif", fn_nullif);
	function_unset(func, FN_CONST);
	function_set(func, FN_DERIVE);
	function_mgr_add(self, func);
}
