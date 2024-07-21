
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_planner.h>
#include <sonata_compiler.h>

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

	// expr
	int rexpr = emit_expr(self, delete->target, delete->returning);

	// add to the returning set
	op2(self, CSET_ADD, delete->rset, rexpr);
	runpin(self, rexpr);

	op1(self, CDELETE, delete->target->id);
}

hot void
emit_delete(Compiler* self, Ast* ast)
{
	// DELETE FROM name [WHERE expr] [RETURNING expr]
	auto delete = ast_delete_of(ast);

	// delete by cursor
	if (! delete->returning)
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
	delete->rset = op1(self, CSET, rpin(self));

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
