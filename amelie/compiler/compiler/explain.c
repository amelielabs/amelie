
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

static void
explain_access(Access* self, Buf* buf)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);

		// target and lock
		buf_format(buf, "  •   {str}.{str} {s} [",
		           record->rel->user,
		           record->rel->name,
		           lock_id_cstr(record->lock));

		// permissions
		auto     first = true;
		uint32_t perm  = record->perm;
		while (perm > 0)
		{
			auto id = permission_next(&perm);
			if (first)
				first = false;
			else
				buf_write(buf, ", ", 2);
			buf_format(buf, "{s}", permission_name_of(id));
		}
		buf_write(buf, "]\n", 2);
	}
}

void
explain_program(Compiler* self, Program* program, Str* user, Str* name)
{
	auto buf = &self->program->explain;
	buf_write(buf, "\n", 1);

	// [function user.function]
	if (name)
		buf_format(buf, "FUNCTION {str}.{str}()\n\n", user, name);

	// main
	buf_format(buf, "main\n");
	op_dump(program, &program->code, buf);

	// pushdown
	if (code_count(&program->code_backend) > 0)
	{
		buf_format(buf, "\npushdown\n");
		op_dump(program, &program->code_backend, buf);
	}

	// access
	auto access = &program->access;
	if (! access_empty(access))
	{
		buf_format(buf, "\naccess\n");
		explain_access(access, buf);
	}

	// calls
	if (! name)
	{
		for (auto i = 0; i < access->list_count; i++)
		{
			auto record = access_at(access, i);
			if (record->lock == LOCK_CALL)
			{
				auto udf = udf_of(record->rel);
				auto udf_program = (Program*)udf->data;
				explain_program(self, udf_program, udf->rel.user, udf->rel.name);
			}
		}
	}
}

void
explain(Compiler* self, Str* user, Str* name)
{
	auto program = self->program;
	explain_program(self, program, user, name);
}
