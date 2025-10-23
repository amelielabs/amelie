
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

bool
parse_while(Stmt* self)
{
	// WHILE expr DO
	//   block
	// END
	auto stmt = ast_while_allocate(self->block);
	self->ast = &stmt->ast;

	// expr
	stmt->expr = parse_expr(self, NULL);

	// DO
	stmt_expect(self, KDO);

	// block
	stmt->block = blocks_add(&self->block->ns->blocks, self->block);
	stmt->block->from = &stmt->from;
	parse_block(self->parser, stmt->block);

	// END
	stmt_expect(self, KEND);

	// iterate all created blocks and copy deps (to vars) to the
	// statements of the outer block
	//
	// this is necessary to receive all vars stmts before executing
	// the while blocks.
	//
	block_copy_deps(self, stmt->block);

	// if while block sends data
	return stmt->block->stmts.last_send != NULL;
}
