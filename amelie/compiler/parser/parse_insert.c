
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

hot static inline Ast*
parse_column_list(Stmt* self, AstInsert* stmt)
{
	auto table = from_first(&stmt->from)->from_table;
	auto columns = table_columns(table);

	// empty column list ()
	if (unlikely(stmt_if(self, ')')))
		return NULL;

	Ast* list = NULL;
	Ast* list_last = NULL;
	for (;;)
	{
		// name
		auto name = stmt_expect(self, KNAME);

		// find column and validate order
		auto column = columns_find(columns, &name->string);
		if (! column)
			stmt_error(self, name, "column does not exists");

		if (list == NULL) {
			list = name;
		} else
		{
			// validate column order
			if (column->order <= list_last->column->order)
				stmt_error(self, name, "column list must be in order");

			list_last->next = name;
		}
		list_last = name;

		// set column
		name->column = column;

		// ,
		if (stmt_if(self, ','))
			continue;

		// )
		stmt_expect(self, ')');
		break;
	}

	return list;
}

hot static inline void
parse_on_conflict(Stmt* self, AstInsert* stmt)
{
	// ON CONFLICT
	if (! stmt_if(self, KON))
		return;

	// CONFLICT
	stmt_expect(self, KCONFLICT);

	// DO
	stmt_expect(self, KDO);

	// NOTHING | ERROR | UPDATE
	auto op = stmt_next(self);
	switch (op->id) {
	case KNOTHING:
	{
		stmt->on_conflict = ON_CONFLICT_NOTHING;
		break;
	}
	case KUPDATE:
	{
		stmt->on_conflict = ON_CONFLICT_UPDATE;

		// SET path = expr [, ... ]
		stmt->update_expr = parse_update_expr(self);

		// [WHERE]
		if (stmt_if(self, KWHERE))
		{
			Expr ctx;
			expr_init(&ctx);
			ctx.from = &stmt->from;
			stmt->update_where = parse_expr(self, &ctx);
		}
		break;
	}
	case KERROR:
		stmt->on_conflict = ON_CONFLICT_ERROR;
		break;
	default:
		stmt_error(self, op, "'NOTHING | ERROR | UPDATE' expected");
		break;
	}
}

hot void
parse_insert(Stmt* self)
{
	// INSERT INTO [user.]name [(column_list)]
	// [GENERATE | VALUES (value, ..), ... | SELECT ...]
	// [ON CONFLICT DO NOTHING | ERROR | UPDATE]
	// [RETURNING expr]
	auto stmt = ast_insert_allocate(self->block);
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// INTO
	auto into = stmt_expect(self, KINTO);

	// table
	parse_from(self, &stmt->from, LOCK_SHARED_RW, PERM_INSERT, false);
	if (from_empty(&stmt->from) || from_is_join(&stmt->from))
		stmt_error(self, into, "table name expected");
	auto target = from_first(&stmt->from);
	if (! target_is_table(target))
		stmt_error(self, into, "table name expected");
	target_set_dml(target, true);

	// ensure index is not defined
	auto table   = target->from_table;
	auto columns = target->columns;
	if (target->from_index)
		if (table_primary(table) != target->from_index)
			stmt_error(self, NULL, "INSERT supports only primary index");

	// prepare values
	auto parser = self->parser;
	stmt->values = set_cache_create(parser->set_cache, &parser->program->sets);
	set_prepare(stmt->values, columns->count, 0, NULL);

	// GENERATE
	if (stmt_if(self, KGENERATE))
	{
		// count
		auto count = stmt_expect(self, KINT);
		parse_row_generate(self, table, stmt->values, count->integer);
	} else
	{
		// (column list)
		Ast* list = NULL;
		bool list_in_use = stmt_if(self, '(');
		if (list_in_use)
			list = parse_column_list(self, stmt);

		auto values = stmt_next(self);
		if (values->id == KVALUES)
		{
			// VALUES (value[, ...])[, ...]
			parse_rows(self, &stmt->from, table, stmt->values, list, list_in_use);
		} else
		if (values->id == KSELECT)
		{
			if (list_in_use)
				stmt_error(self, values, "SELECT using column list is not supported");

			// rewrite INSERT INTO SELECT as separate statement, columns will be
			// validated during the emit
			auto select = stmt_allocate(parser, &parser->lex, self->block);
			select->id  = STMT_SELECT;
			stmts_insert(&self->block->stmts, self, select);
			deps_add_stmt(&self->deps, select);

			auto select_ref = parse_select(select, NULL, false);
			select->ret = &select_ref->ret;
			select->ast = &select_ref->ast;
			select->ast->pos_start = values->pos_start;
			select->ast->pos_end   = values->pos_end;
			parse_select_resolve(select);
			stmt->select = select;
		} else {
			stmt_error(self, values, "'VALUES | SELECT' expected");
		}
	}

	// ON CONFLICT
	parse_on_conflict(self, stmt);

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
	{
		parse_returning(&stmt->ret, self, NULL);
		parse_returning_resolve(&stmt->ret, self, &stmt->from);

		// convert insert to upsert ON CONFLICT ERROR to support returning
		if (stmt->on_conflict == ON_CONFLICT_NONE)
			stmt->on_conflict = ON_CONFLICT_ERROR;

		// require SELECT permission
		access_add(&self->parser->program->access, &table->rel,
		           LOCK_SHARED, PERM_SELECT);
	}

	// update requested permissions to UPDATE
	if (stmt->on_conflict == ON_CONFLICT_UPDATE)
	{
		auto access = access_find(&self->parser->program->access,
		                          &table->config->user,
		                          &table->config->name);
		access->perm |= PERM_UPDATE;
	}
}
