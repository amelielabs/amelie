
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
	// FOR alias IN from_target DO
	//   block
	// END
	auto stmt = ast_for_allocate(self->block);
	self->ast = &stmt->ast;

	if (self->assign)
		stmt_error(self, NULL, "FOR cannot be assigned");

	// alias
	auto as = stmt_expect(self, KNAME);

	// IN
	stmt_expect(self, KIN);

	// identical to FROM target
	auto target = parse_from_add(self, &stmt->targets, ACCESS_RO, &as->string, false);
	if (target->type == TARGET_TABLE)
		stmt_error(self, NULL, "tables are not supported here");

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
