
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>

static inline void
emit_delete_on_match(Compiler* self, void* arg)
{
	AstDelete* delete = arg;
	op1(self, CDELETE, delete->target->id);
}

static inline void
emit_delete_on_match_returning(Compiler* self, void* arg)
{
	AstDelete* delete = arg;

	// push expr and set column type
	for (auto as = delete->ret.list; as; as = as->next)
	{
		auto column = as->r->column;

		// expr
		int rexpr = emit_expr(self, delete->target, as->l);
		int rt = rtype(self, rexpr);
		column_set_type(column, rt, type_sizeof(rt));
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
	}

	// add to the returning set
	op1(self, CSET_ADD, delete->rset);

	op1(self, CDELETE, delete->target->id);
}

hot void
emit_delete(Compiler* self, Ast* ast)
{
	// DELETE FROM name [WHERE expr] [RETURNING expr]
	auto delete = ast_delete_of(ast);

	// delete by cursor
	if (! returning_has(&delete->ret))
	{
		scan(self, delete->target,
		     NULL,
		     NULL,
		     delete->expr_where,
		     emit_delete_on_match,
		     delete);
		return;
	}

	// RETURNING expr

	// create returning set
	delete->rset =
		op3(self, CSET, rpin(self, TYPE_SET),
		    delete->ret.count, 0);

	scan(self, delete->target,
	     NULL,
	     NULL,
	     delete->expr_where,
	     emit_delete_on_match_returning,
	     delete);

	// CRESULT (return set)
	op1(self, CRESULT, delete->rset);
	runpin(self, delete->rset);
	delete->rset = -1;
}
