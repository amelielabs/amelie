
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_semantic.h>
#include <monotone_compiler.h>

static inline void
emit_delete_on_match(Compiler* self, void *arg)
{
	Target* target = arg;
	op1(self, CDELETE, target->id);
}

hot void
emit_delete(Compiler* self, Ast* ast)
{
	// DELETE FROM name [WHERE expr]
	auto delete = ast_delete_of(ast);
	auto stmt   = self->current;

	// distributed table

	// delete by cursor
	scan(self, delete->target,
	     NULL,
	     NULL,
	     delete->expr_where,
	     emit_delete_on_match,
	     delete->target);

	// CREADY
	op2(self, CREADY, stmt->order, -1);

	if (delete->target->table->config->reference)
	{
		// get route by order
		auto route = router_at(self->router, 0);

		// get the request
		auto req = dispatch_add(self->dispatch, stmt->order, route);

		// append code
		code_copy(&req->code, &self->code_stmt);

	} else
	{
		// execute code on each shard
		dispatch_copy(self->dispatch, &self->code_stmt, stmt->order);
	}

	// coordinator
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);
}
