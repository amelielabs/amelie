
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

AstMatching*
parse_matching(Stmt* self, From* from, Table* table)
{
	// MATCHING column TO expr TOP expr
	auto stmt = ast_matching_allocate(from, self->block);
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// mark as closing
	self->block->stmts.last_send = self;

	// column
	auto name = stmt_expect(self, KNAME);
	auto column = columns_find(table_columns(table), &name->string);
	if (! column)
		stmt_error(self, name, "column does not exists");

	// ensure column is vector
	if (column->type != TYPE_VECTOR)
		stmt_error(self, name, "column type is not vector");

	stmt->column = column;
	stmt->table  = table;

	// add table target
	auto target = target_allocate();
	target->type          = TARGET_TABLE;
	target->from_lock     = LOCK_SHARED;
	target->from_table    = table;
	target->from_snapshot = table_main(table);
	target->columns       = &table->config->columns;
	str_set_str(&target->name, &table->config->name);
	access_add(&self->parser->program->access, &table->rel, LOCK_SHARED, PERM_SELECT);
	from_add(&stmt->from, target);

	// returning
	parse_returning_star(&stmt->ret);
	parse_returning_resolve(&stmt->ret, self, &stmt->from);

	// TO
	stmt_expect(self, KTO);

	// expr
	Expr ctx;
	expr_init(&ctx);
	ctx.select = false;
	ctx.from   = &stmt->from;
	stmt->expr_to = parse_expr(self, &ctx);

	auto ref = refs_add(&self->refs, &stmt->from, stmt->expr_to, -1);
	ref->not_null = true;

	// TOP
	stmt_expect(self, KTOP);
	stmt->expr_top = stmt_expect(self, KINT);
	if (stmt->expr_top->integer < 1)
		stmt_error(self, stmt->expr_top, "invalid top value");

	return stmt;
}
