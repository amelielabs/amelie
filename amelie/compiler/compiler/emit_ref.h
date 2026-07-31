#pragma once

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

static inline int
emit_refs(Compiler* self)
{
	auto stmt = self->current;

	// push references
	auto ref = stmt->refs.list;
	while (ref)
	{
		if (ref->ast) {
			int type;
			if (ref->ast->id == KVAR)
			{
				auto var = ref->ast->var;
				op3(self, CPUSH_VAR, var->order, var->is_arg, ref->not_null);
				type = var->type;
			} else
			{
				auto r = emit_expr(self, ref->from, ref->ast);
				op3(self, CPUSH_REF, r, 0, ref->not_null);
				type = rtype(self, r);
				runpin(self, r);
			}
			if (ref->column)
			{
				if (ref->column->type != type)
					stmt_error(stmt, ref->ast, "expected '{s}'", type_of(ref->column->type));
			}

		} else {
			op3(self, CPUSH_REF, ref->r, 1, ref->not_null);
		}
		ref = ref->next;
	}

	return stmt->refs.count;
}
