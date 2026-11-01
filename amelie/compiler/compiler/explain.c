
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
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_copy.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

static void
explain_access(Access* self, Buf* buf)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);

		// target
		buf_format(buf, "  •   {str}.{str} [", record->rel->user,
		           record->rel->name);

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
explain(Compiler* self, Program* program, Udf* udf, bool calls)
{
	auto buf = &self->explain;
	buf_write(buf, "\n", 1);

	// [function user.function]
	if (udf)
		buf_format(buf, "FUNCTION {str}.{str}\n\n", udf->rel.user,
		           udf->rel.name);

	// main
	auto access = &program->access;
	auto multi_user = access->multi_user;
	if (access->list_count > 0 && !multi_user)
	{
		auto first = access_at(access, 0);
		if (! str_compare(&self->parser.local->user, first->rel->user))
			multi_user = true;
	}

	if (multi_user)
		buf_format(buf, "main [multi-user]\n");
	else
		buf_format(buf, "main\n");

	op_dump(program, &program->code, buf);

	// access
	if (! access_empty(access))
	{
		buf_format(buf, "\naccess\n");
		explain_access(access, buf);
	}

	// calls
	if (calls)
	{
		for (auto i = 0; i < access->list_count; i++)
		{
			auto record = access_at(access, i);
			if (record->perm & PERM_EXECUTE)
			{
				auto ref = udf_of(record->rel);
				auto ref_program = (Program*)ref->data;
				if (ref == udf)
					continue;
				explain(self, ref_program, ref, false);
			}
		}
	}
}
