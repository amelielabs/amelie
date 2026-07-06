
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
#include <amelie_commit.h>
#include <amelie_func.h>

void
fn_register(Functions* self)
{
	fn_system_register(self);
	fn_cast_register(self);
	fn_null_register(self);
	fn_json_register(self);
	fn_string_register(self);
	fn_regexp_register(self);
	fn_math_register(self);
	fn_misc_register(self);
	fn_time_register(self);
	fn_vector_register(self);
}
