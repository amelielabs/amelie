
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
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

hot static void
parse_row_list(Stmt* self, AstInsert* stmt, Ast* list)
{
	auto table = targets_outer(&stmt->targets)->from_table;

	// (
	stmt_expect(self, '(');

	// prepare row
	auto row = set_reserve(stmt->values);

	// set next sequence value for identity columns
	uint64_t seq = sequence_next(&table->seq);

	// value, ...
	auto columns = table_columns(table);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];

		// DEFAULT | value
		Ast* value = stmt_if(self, KDEFAULT);
		if (!value && list && list->column->order == column->order)
		{
			// parse column value
			value = parse_value(self, column, column_value);

			// ,
			list = list->next;
			if (list)
				stmt_expect(self, ',');

		} else
		{
			// IDENTITY, RANDOM or DEFAULT
			parse_value_default(self, column, column_value, seq);
		}

		// ensure NOT NULL constraint
		parse_value_validate(self, column, column_value, value);
	}

	// )
	stmt_expect(self, ')');
}

hot static inline void
parse_row(Stmt* self, AstInsert* stmt)
{
	auto table = targets_outer(&stmt->targets)->from_table;

	// (
	stmt_expect(self, '(');

	// prepare row
	auto row = set_reserve(stmt->values);

	uint64_t seq = sequence_next(&table->seq);

	// value, ...
	auto columns = table_columns(table);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];

		// DEFAULT | value
		Ast* value = stmt_if(self, KDEFAULT);
		if (! value)
		{
			// parse column value
			value = parse_value(self, column, column_value);
		} else
		{
			// IDENTITY, RANDOM or DEFAULT
			parse_value_default(self, column, column_value, seq);
		}

		// ensure NOT NULL constraint
		parse_value_validate(self, column, column_value, value);

		// ,
		if (stmt_if(self, ','))
		{
			if (list_is_last(&columns->list, &column->link))
				stmt_error(self, NULL, "row has incorrect number of columns");
			continue;
		}
	}

	// )
	stmt_expect(self, ')');
}

hot static inline Ast*
parse_column_list(Stmt* self, AstInsert* stmt)
{
	auto table = targets_outer(&stmt->targets)->from_table;
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
parse_generate(Stmt* self, AstInsert* stmt)
{
	// insert into () values (), ...
	auto table = targets_outer(&stmt->targets)->from_table;

	// GENERATE count
	auto count = stmt_expect(self, KINT);

	auto columns = table_columns(table);
	for (auto i = 0; i < count->integer; i++)
	{
		// prepare row
		auto row = set_reserve(stmt->values);

		// set next sequence value for identity columns
		uint64_t seq = sequence_next(&table->seq);

		// value, ...
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			auto column_value = &row[column->order];

			// IDENTITY, RANDOM or DEFAULT
			parse_value_default(self, column, column_value, seq);

			// ensure NOT NULL constraint
			parse_value_validate(self, column, column_value, NULL);
		}
	}
}

hot void
parse_resolved(Stmt* self)
{
	auto stmt = ast_insert_of(self->ast);
	stmt->on_conflict = ON_CONFLICT_UPDATE;
	// handle insert as upsert and generate
	//
	// SET <column> = <resolved column expression> [, ...]
	auto table = targets_outer(&stmt->targets)->from_table;
	auto columns = table_columns(table);
	stmt->update_expr = parse_update_resolved(self, columns);
	if (! stmt->update_expr)
		stmt->on_conflict = ON_CONFLICT_ERROR;
}

hot static inline void
parse_on_conflict(Stmt* self, AstInsert* stmt)
{
	// ON CONFLICT
	if (! stmt_if(self, KON))
	{
		// if table has resvoled conlumns and no explicit ON CONFLICT clause
		// then handle as ON CONFLICT DO RESOLVE
		auto table = targets_outer(&stmt->targets)->from_table;
		if (table->config->columns.count_resolved > 0)
			parse_resolved(self);
		return;
	}

	// CONFLICT
	stmt_expect(self, KCONFLICT);

	// DO
	stmt_expect(self, KDO);

	// NOTHING | ERROR | UPDATE | RESOLVE
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
			ctx.targets = &stmt->targets;
			stmt->update_where = parse_expr(self, &ctx);
		}
		break;
	}
	case KRESOLVE:
	{
		// handle insert as upsert and generate
		//
		// SET <column> = <resolved column expression> [, ...]
		parse_resolved(self);
		break;
	}
	case KERROR:
		stmt->on_conflict = ON_CONFLICT_ERROR;
		break;
	default:
		stmt_error(self, op, "'NOTHING | ERROR | UPDATE | RESOLVE' expected");
		break;
	}
}

hot void
parse_generated(Stmt* self)
{
	auto stmt = ast_insert_of(self->ast);
	auto table = targets_outer(&stmt->targets)->from_table;
	auto columns = table_columns(table);

	// create a target to iterate inserted values to create new rows
	// using the generated columns
	auto target = target_allocate(&self->order_targets);
	target->type         = TARGET_VALUES;
	target->ast          = targets_outer(&stmt->targets)->ast;
	target->from_columns = targets_outer(&stmt->targets)->from_columns;
	target->name         = table->config->name;
	targets_add(&stmt->targets_generated, target);

	// parse generated columns expressions
	auto lex_origin = self->lex;
	Lex lex;
	lex_init(&lex, keywords_alpha);
	self->lex = &lex;

	Ast* tail = NULL;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (str_empty(&column->constraints.as_stored))
			continue;
		lex_init(&lex, keywords_alpha);
		lex_start(&lex, &column->constraints.as_stored);
		Expr ctx =
		{
			.aggs    = NULL,
			.select  = false,
			.targets = &stmt->targets_generated
		};

		// op(column, expr)
		auto op = ast(KSET);
		op->l = ast(0);
		op->l->column = column;
		op->r = parse_expr(self, &ctx);

		if (! stmt->generated_columns)
			stmt->generated_columns = op;
		else
			tail->next = op;
		tail = op;
	}

	self->lex = lex_origin;
}

hot void
parse_insert(Stmt* self)
{
	// INSERT INTO name [(column_list)]
	// [GENERATE | VALUES (value, ..), ... | SELECT ...]
	// [ON CONFLICT DO NOTHING | ERROR | UPDATE | RESOLVE]
	// [RETURNING expr [FORMAT name]]
	auto stmt = ast_insert_allocate();
	self->ast = &stmt->ast;

	// INTO
	auto into = stmt_expect(self, KINTO);

	// table
	parse_from(self, &stmt->targets, ACCESS_RW, false);
	if (targets_empty(&stmt->targets) || targets_is_join(&stmt->targets))
		stmt_error(self, into, "table name expected");
	auto target = targets_outer(&stmt->targets);
	if (! target_is_table(target))
		stmt_error(self, into, "table name expected");
	auto columns = target->from_columns;

	// prepare values
	stmt->values = set_cache_create(self->values_cache);
	set_prepare(stmt->values, columns->count, 0, NULL);

	// GENERATE
	if (stmt_if(self, KGENERATE))
	{
		parse_generate(self, stmt);
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
			for (;;)
			{
				if (list_in_use)
					parse_row_list(self, stmt, list);
				else
					parse_row(self, stmt);
				if (! stmt_if(self, ','))
					break;
			}
		} else
		if (values->id == KSELECT)
		{
			if (list_in_use)
				stmt_error(self, values, "SELECT using column list is not supported");

			// rewrite INSERT INTO SELECT as CTE statement, columns will be
			// validated during the emit
			auto cte = stmt_allocate(self->db, self->function_mgr, self->local,
			                         self->lex,
			                         self->data,
			                         self->access,
			                         self->values_cache,
			                         self->json,
			                         self->stmt_list,
			                         self->args);
			cte->id = STMT_SELECT;
			stmt_list_insert(self->stmt_list, self, cte);
			auto select = parse_select(cte, NULL, false);
			select->ast.pos_start = values->pos_start;
			select->ast.pos_end   = values->pos_end;
			cte->ast         = &select->ast;
			cte->cte_columns = &select->ret.columns;
			parse_select_resolve(cte);
			stmt->select = cte;
		} else {
			stmt_error(self, values, "'VALUES | SELECT' expected");
		}
	}

	// create a list of generated columns expressions
	if (columns->count_stored > 0)
		parse_generated(self);

	// ON CONFLICT
	parse_on_conflict(self, stmt);

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
	{
		parse_returning(&stmt->ret, self, NULL);
		parse_returning_resolve(&stmt->ret, self, &stmt->targets);

		// convert insert to upsert ON CONFLICT ERROR to support returning
		if (stmt->on_conflict == ON_CONFLICT_NONE)
			stmt->on_conflict = ON_CONFLICT_ERROR;
	}
}
