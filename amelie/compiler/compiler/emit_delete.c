
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
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

static inline void
emit_delete_on_match(Scan* self)
{
	// DELETE by cursor
	auto target = from_first(self->from);
	op1(self->compiler, CDELETE, target->rcursor);
}

static inline void
emit_delete_on_match_returning(Scan* self)
{
	auto       cp     = self->compiler;
	AstDelete* delete = self->on_match_arg;

	// push expr and set column type
	for (auto as = delete->ret.list; as; as = as->next)
	{
		auto column = as->r->column;
		auto type = emit_push(cp, self->from, as->l);
		column_set_type(column, type, type_sizeof(type));
	}

	// add to the returning set
	op1(cp, CSET_ADD, delete->rset);

	// DELETE by cursor
	auto target = from_first(self->from);
	op1(cp, CDELETE, target->rcursor);
}

hot int
emit_delete(Compiler* self, Ast* ast)
{
	// DELETE FROM name [WHERE expr] [RETURNING expr]
	auto delete = ast_delete_of(ast);

	// set target origin
	auto target = from_first(&delete->from);
	target_set_origin(target, self->origin);

	// delete by cursor
	if (! returning_has(&delete->ret))
	{
		scan(self, &delete->from,
		     NULL,
		     NULL,
		     delete->expr_where,
		     emit_delete_on_match,
		     delete);
		return -1;
	}

	// RETURNING expr

	// create returning set
	delete->rset = op3pin(self, CSET, TYPE_STORE, delete->ret.count, 0);

	scan(self, &delete->from,
	     NULL,
	     NULL,
	     delete->expr_where,
	     emit_delete_on_match_returning,
	     delete);

	// returning set
	return delete->rset;
}
