
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
#include <amelie_engine>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

bool
parse_for(Stmt* self)
{
	// FOR alias IN from_target DO
	//   block
	// END
	auto stmt = ast_for_allocate(self->block);
	self->ast = &stmt->ast;

	// alias
	auto as = stmt_expect(self, KNAME);

	// IN
	stmt_expect(self, KIN);

	// identical to FROM target
	auto target = parse_from_add(self, &stmt->from, ACCESS_RO, &as->string, false);
	if (target->type == TARGET_TABLE)
		stmt_error(self, NULL, "tables are not supported here");

	// LOOP | DO
	if (! stmt_if(self, KLOOP))
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
	// the for blocks
	//
	block_copy_deps(self, stmt->block, false);

	// if for block sends data
	return stmt->block->stmts.last_send != NULL;
}
