
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
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

void
explain(Compiler* self, Str* function)
{
	auto program = self->program;
	auto buf = &self->program->explain;

	encode_obj(buf);

	// [function]
	if (function)
	{
		encode_raw(buf, "function", 8);
		encode_string(buf, function);
	}

	// main
	encode_raw(buf, "main", 4);
	op_dump(program, &program->code, buf);

	// pushdown
	if (code_count(&program->code_backend) > 0)
	{
		encode_raw(buf, "pushdown", 8);
		op_dump(program, &program->code_backend, buf);
	}

	// access
	auto access = &program->access;
	encode_raw(buf, "access", 6);
	auto has_call = access_encode(access, buf);

	// calls
	if (!function && has_call)
	{
		encode_raw(buf, "calls", 5);
		encode_array(buf);
		for (auto i = 0; i < access->list_count; i++)
		{
			auto record = access_at(access, i);
			if (record->lock == LOCK_CALL)
			{
				auto udf = udf_of(record->rel);
				auto udf_program = (Program*)udf->data;
				buf_write_buf(buf, &udf_program->explain);
			}
		}
		encode_array_end(buf);
	}

	encode_obj_end(buf);
}
