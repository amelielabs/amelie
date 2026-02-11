
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
	stmt->block = blocks_add(&self->block->ns->blocks, self->block, self);
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
	block_copy_deps(self, stmt->block, false);

	// if while block sends data
	return stmt->block->stmts.last_send != NULL;
}
