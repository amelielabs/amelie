
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
parse_for(Stmt* self)
{
	// FOR target IN cte DO
	//   block
	// END
	auto stmt = ast_for_allocate(self->block);
	self->ast = &stmt->ast;

	// target
	auto name = stmt_expect(self, KNAME);

	// IN
	auto in = stmt_expect(self, KIN);

	// cte
	auto cte = stmt_allocate(self->parser, &self->parser->lex, self->block);
	stmts_insert(&self->block->stmts, self, cte);
	stmt->cte = cte;

	// parse stmt
	parse_stmt(self->parser, cte);

	// set cte returning columns
	Returning* ret = NULL;
	switch (cte->id) {
	case STMT_INSERT:
		ret = &ast_insert_of(cte->ast)->ret;
		break;
	case STMT_UPDATE:
		ret = &ast_update_of(cte->ast)->ret;
		break;
	case STMT_DELETE:
		ret = &ast_delete_of(cte->ast)->ret;
		break;
	case STMT_SELECT:
		ret = &ast_select_of(cte->ast)->ret;
		break;
	default:
		break;
	}
	if (ret == NULL || !returning_has(ret))
		stmt_error(self, in, "expected Returning DML or SELECT");

	cte->cte = ctes_add(&self->block->ctes, self, &name->string);
	cte->cte->columns = &ret->columns;

	auto target = target_allocate();
	target->type         = TARGET_CTE;
	target->from_cte     = cte;
	target->from_columns = &ret->columns;
	str_set_str(&target->name, &name->string);
	targets_add(&stmt->targets, target);

	// LOOP | DO
	if (! stmt_if(self, KLOOP))
		stmt_expect(self, KDO);

	// block
	stmt->block = blocks_add(&self->parser->blocks, self->block);
	stmt->block->targets = &stmt->targets;
	parse_block(self->parser, stmt->block);

	// END
	stmt_expect(self, KEND);

	// if for block sends data
	return stmt->block->stmts.last_send != NULL;
}
