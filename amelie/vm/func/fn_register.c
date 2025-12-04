
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
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>

void
fn_register(FunctionMgr* mgr)
{
	fn_system_register(mgr);
	fn_cast_register(mgr);
	fn_null_register(mgr);
	fn_json_register(mgr);
	fn_string_register(mgr);
	fn_regexp_register(mgr);
	fn_math_register(mgr);
	fn_misc_register(mgr);
	fn_time_register(mgr);
	fn_vector_register(mgr);
}
