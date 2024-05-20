
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>
#include <indigo_semantic.h>
#include <indigo_compiler.h>

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
		op_relocate(&req->code, &self->code_stmt);

	} else
	{
		// copy statement code to each shard
		compiler_distribute(self);
	}

	// coordinator
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);
}
