
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

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
			parse_value_add(&stack);
			break;
		case KNULL:
			parse_value_add(&stack);
			break;
		case KTRUE:
		case KFALSE:
			parse_value_add(&stack);
			break;
		case KINT:
			parse_value_add(&stack);
			break;
		case KREAL:
			parse_value_add(&stack);
			break;
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
	auto def = table_def(stmt->table);
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
	auto def = table_def(stmt->table);
	auto row = ast_row_allocate();

	// (
	if (! stmt_if(self, '('))
		error("expected '('");

	// set next serial value
	uint64_t serial = serial_next(&stmt->table->serial);

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
	auto def = table_def(stmt->table);

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

hot void
parse_insert(Stmt* self, bool unique)
{
	// [INSERT|REPLACE] INTO name [(column_list)]
	// [VALUES] (value, ..), ...
	auto stmt = ast_insert_allocate();
	self->ast = &stmt->ast;

	// insert
	stmt->unique = unique;

	// INTO
	if (! stmt_if(self, KINTO))
		error("INSERT <INTO> expected");

	// name
	// schema.name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		error("INSERT INTO <name> expected");
	stmt->table = table_mgr_find(&self->db->table_mgr, &schema, &name, true);

	// GENERATE
	if (stmt_if(self, KGENERATE))
	{
		stmt->generate = true;

		// count
		stmt->generate_count = stmt_if(self, KINT);
		if (! stmt->generate_count)
			error("GENERATE <count> expected");

		// batch
		stmt->generate_batch = stmt_if(self, KINT);
		if (! stmt->generate_batch)
			error("GENERATE count <batch count> expected");

		return;
	}

	// (column list)
	Ast* list = NULL;
	bool list_in_use = stmt_if(self, '(');
	if (list_in_use)
		list = parse_column_list(self, stmt);

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

	} else
	{
		// one column case
		auto def = table_def(stmt->table);
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
}
