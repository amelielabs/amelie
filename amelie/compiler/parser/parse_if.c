
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

static void
parse_if_block(Stmt* self)
{
	auto parser = self->parser;
	auto stmt = ast_if_of(self->ast);

	// IF expr
	auto cond = ast_if_cond_allocate();
	ast_list_add(&stmt->conds, &cond->ast);
	cond->expr = parse_expr(self, NULL);

	/*
	cond->stmt = stmt_allocate(self->parser, self->lex, self->block);
	cond->stmt->id  = STMT_SELECT;
	cond->stmt->ast = &parse_select_expr(self)->ast;
	*/

	// THEN
	stmt_expect(self, KTHEN);

	// block
	cond->block = blocks_add(&parser->blocks, self->block);
	parse_block(parser, cond->block);
}

void
parse_if(Stmt* self)
{
	// IF expr THEN
	//   block
	// [ELSIF expr THEN
	//   block]
	// [ELSE
	//   block]
	// END
	auto stmt = ast_if_allocate(self->block);
	self->ast = &stmt->ast;

	// IF expr THEN block [END]
	parse_if_block(self);

	for (;;)
	{
		// [ELSIF expr THEN block]
		if (stmt_if(self, KELSIF)) {
			parse_if_block(self);
			continue;
		}

		// [ELSE block]
		if (stmt_if(self, KELSE))
		{
			// block
			stmt->cond_else = blocks_add(&self->parser->blocks, self->block);
			parse_block(self->parser, stmt->cond_else);
		}

		stmt_expect(self, KEND);
		break;
	}
}
