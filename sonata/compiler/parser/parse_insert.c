
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
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
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

hot static inline void
parse_value_add(AstStack* stack)
{
	if (ast_head(stack))
		ast_head(stack)->integer++;
}

hot static inline AstValue*
parse_value(Stmt* self)
{
	AstStack stack;
	ast_stack_init(&stack);

	auto value = ast_value_allocate();
	Ast* last  = NULL;
	bool done  = false;
	while (! done)
	{
		auto ast = stmt_next(self);
		switch (ast->id) {
		case '[':
			parse_value_add(&stack);
			ast_push(&stack, ast);
			break;
		case ']':
		{
			auto head = ast_head(&stack);
			if (unlikely(!head || head->id != '['))
				goto done;
			ast_pop(&stack);
			continue;
		}
		case '{':
			parse_value_add(&stack);
			ast_push(&stack, ast);
			break;
		case '}':
		{
			auto head = ast_head(&stack);
			if (unlikely(!head || head->id != '{'))
				goto done;
			if (unlikely((head->integer % 2) != 0))
				error("incorrect {} object");
			head->integer /= 2;
			ast_pop(&stack);
			continue;
		}
		case ':':
		{
			auto head = ast_head(&stack);
			if (!head || head->id != '{')
				goto done;
			continue;
		}
		case ',':
		{
			auto head = ast_head(&stack);
			if (! head)
				goto done;
			continue;
		}
		case KSTRING:
		case KNULL:
		case KTRUE:
		case KFALSE:
		case KINT:
		case KREAL:
			parse_value_add(&stack);
			break;
		case '-':
		{
			auto next = stmt_if(self, KINT);
			if (next)
			{
				parse_value_add(&stack);
				ast = next;
				next->integer = -next->integer;
				break;
			}
			next = stmt_if(self, KREAL);
			if (next)
			{
				parse_value_add(&stack);
				ast = next;
				next->real = -next->real;
				break;
			}
			stmt_push(self, ast);
			goto done;
		}

		default:
			goto done;
		}

		if (value->expr == NULL)
			value->expr = ast;
		else
			last->r = ast;
		last = ast;

		continue;
done:;
		done = true;
		stmt_push(self, ast);
	}

	if (unlikely(ast_head(&stack)))
		error("bad row value");

	return value;
}

hot static AstRow*
parse_row(Stmt* self, AstInsert* stmt)
{
	auto def = table_def(stmt->target->table);
	auto row = ast_row_allocate();

	// (
	if (! stmt_if(self, '('))
		error("expected '('");

	// value, ...
	Ast* last = NULL;
	auto column = def->column;
	for (; column; column = column->next)
	{
		// value
		auto value = parse_value(self);
		if (unlikely(value->expr == NULL))
			error("bad row value");

		// ensure NOT NULL constraint
		if (column->constraint.not_null)
			if (unlikely(value->expr->id == KNULL))
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));

		// add value to the row
		if (row->list == NULL)
			row->list = &value->ast;
		else
			last->next = &value->ast;
		row->list_count++;
		last = &value->ast;

		// ,
		if (stmt_if(self, ','))
		{
			if (! column->next)
				error("row has incorrect number of columns");
			continue;
		}
	}

	// )
	if (! stmt_if(self, ')'))
		error("expected ')'");

	return row;
}

hot static AstRow*
parse_row_list(Stmt* self, AstInsert* stmt, Ast* list)
{
	auto def = table_def(stmt->target->table);
	auto row = ast_row_allocate();

	// (
	if (! stmt_if(self, '('))
		error("expected '('");

	// set next serial value
	uint64_t serial = serial_next(&stmt->target->table->serial);

	// value, ...
	Ast* last = NULL;
	auto column = def->column;
	for (; column; column = column->next)
	{
		AstValue* value;
		if (list && list->column->order == column->order)
		{
			// value
			value = parse_value(self);
			if (unlikely(value->expr == NULL))
				error("bad row value");

			list = list->next;

			// ,
			if (list) {
				if (! stmt_if(self, ','))
					error("row has incorrect number of columns");
			}

		} else
		{
			// value is not provided
			value = ast_value_allocate();

			// GENERATED
			auto cons = &column->constraint;
			if (cons->generated == GENERATED_SERIAL)
			{
				// set serial value
				value->expr = ast(KINT);
				value->expr->integer = serial;
			}

			if (value->expr == NULL)
			{
				if (! str_empty(&cons->expr))
				{
					// DEFAULT
					auto lex_prev = self->lex;
					Lex lex;
					lex_init(&lex, lex_prev->keywords);
					lex_start(&lex, &cons->expr);
					self->lex = &lex;
					value->expr = stmt_next(self);
					self->lex = lex_prev;
				} else
				{
					// SET NULL
					value->expr = ast(KNULL);
				}
			}
		}

		// ensure NOT NULL constraint
		if (column->constraint.not_null)
			if (unlikely(value->expr->id == KNULL))
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));

		// add value to the row
		if (row->list == NULL)
			row->list = &value->ast;
		else
			last->next = &value->ast;
		row->list_count++;
		last = &value->ast;
	}

	// )
	if (! stmt_if(self, ')'))
		error("expected ')'");

	return row;
}

hot static inline Ast*
parse_column_list(Stmt* self, AstInsert* stmt)
{
	auto def = table_def(stmt->target->table);

	// empty column list ()
	if (unlikely(stmt_if(self, ')')))
		return NULL;

	Ast* list = NULL;
	Ast* list_last = NULL;
	for (;;)
	{
		// name
		auto name = stmt_if(self, KNAME);
		if (unlikely(! name))
			error("INSERT INTO name (<column name> expected");

		// find column and validate order
		auto column = def_find_column(def, &name->string);
		if (! column)
			error("<%.*s> column does not exists", str_size(&name->string),
			      str_of(&name->string));

		if (list == NULL) {
			list = name;
		} else
		{
			// validate column order
			if (column->order <= list_last->column->order)
				error("column list must be in order");

			list_last->next = name;
		}
		list_last = name;

		// set column
		name->column = column;

		// ,
		if (stmt_if(self, ','))
			continue;

		// )
		if (! stmt_if(self, ')'))
			error("expected ')'");

		break;
	}

	return list;
}

hot static inline void
parse_values(Stmt* self, AstInsert* stmt, bool list_in_use, Ast* list)
{
	// [VALUES]
	if (stmt_if(self, KVALUES))
	{
		// (value, ...), ...
		Ast* last = NULL;
		for (;;)
		{
			AstRow* row;
			if (list_in_use)
				row = parse_row_list(self, stmt, list);
			else
				row = parse_row(self, stmt);
			if (stmt->rows == NULL)
				stmt->rows = &row->ast;
			else
				last->next = &row->ast;
			last = &row->ast;
			stmt->rows_count++;

			if (stmt_if(self, ','))
				continue;

			break;
		}

		return;
	}

	// one column case
	auto def = table_def(stmt->target->table);
	if (unlikely(list || def->column_count > 1))
		error("INSERT INTO <VALUES> expected");

	// no constraints applied

	// value, ...
	Ast* last = NULL;
	for (;;)
	{
		// row
		auto row = ast_row_allocate();

		// value
		auto value = parse_value(self);
		if (unlikely(value->expr == NULL))
			error("bad row value");
		row->list = &value->ast;
		row->list_count++;

		if (stmt->rows == NULL)
			stmt->rows = &row->ast;
		else
			last->next = &row->ast;
		last = &row->ast;
		stmt->rows_count++;

		if (stmt_if(self, ','))
			continue;

		break;
	}
}

hot static inline void
parse_generate(Stmt* self, AstInsert* stmt)
{
	// insert into () values (), ...

	// GENERATE count
	auto count = stmt_if(self, KINT);
	if (! count)
		error("GENERATE <count> expected");

	auto lbr = ast('(');
	auto rbr = ast(')');

	Ast* last = NULL;
	for (uint64_t i = 0; i < count->integer; i++)
	{
		// (), ...
		stmt_push(self, rbr);
		stmt_push(self, lbr);

		auto row = parse_row_list(self, stmt, NULL);
		if (stmt->rows == NULL)
			stmt->rows = &row->ast;
		else
			last->next = &row->ast;
		last = &row->ast;
		stmt->rows_count++;
	}
}

hot static inline void
parse_on_conflict(Stmt* self, AstInsert* stmt)
{
	// ON CONFLICT
	if (! stmt_if(self, KON))
		return;

	// CONFLICT
	if (! stmt_if(self, KCONFLICT))
		error("INSERT VALUES ON <CONFLICT> expected");

	// DO
	if (! stmt_if(self, KDO))
		error("INSERT VALUES ON CONFLICT <DO> expected");

	// REPLACE | NOTHING | UPDATE
	auto op = stmt_next(self);
	switch (op->id) {
	case KREPLACE:
		stmt->unique = false;
		break;
	case KNOTHING:
		stmt->on_conflict = ON_CONFLICT_UPDATE_NONE;
		break;
	case KUPDATE:
	{
		stmt->on_conflict = ON_CONFLICT_UPDATE;

		// SET path = expr [, ... ]
		stmt->update_expr = parse_update_expr(self);

		// [WHERE]
		if (stmt_if(self, KWHERE))
			stmt->update_where = parse_expr(self, NULL);
		break;
	}
	default:
		error("INSERT VALUES ON CONFLICT DO "
		      "<REPLACE | NOTHING | UPDATE> expected");
		break;
	}
}

hot void
parse_insert(Stmt* self, bool unique)
{
	// [INSERT|REPLACE] INTO name [(column_list)]
	// [GENERATE | VALUES] (value, ..), ...
	// [ON CONFLICT DO REPLACE | NOTHING | UPDATE]
	auto stmt = ast_insert_allocate();
	self->ast = &stmt->ast;

	// insert
	stmt->unique = unique;

	// INTO
	if (! stmt_if(self, KINTO))
		error("INSERT <INTO> expected");

	// table
	int level = target_list_next_level(&self->target_list);
	stmt->target = parse_from(self, level);
	if (stmt->target->table == NULL || stmt->target->next_join)
		error("INSERT INTO <table> expected");

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

		// [VALUES] | expr
		parse_values(self, stmt, list_in_use, list);
	}

	// ON CONFLICT
	parse_on_conflict(self, stmt);
}
