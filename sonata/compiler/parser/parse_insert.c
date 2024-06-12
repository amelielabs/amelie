
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
#include <sonata_user.h>
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
parse_value(Stmt* self)
{
	// get the begining of current text position
	auto lex = self->lex;
	auto pos = lex->pos;
	while (lex->backlog)
	{
		pos = lex->backlog->pos;
		lex->backlog = lex->backlog->prev;
	}

	// parse and encode json value
	json_reset(self->json);
	Str in;
	str_set(&in, pos, self->lex->end - pos);
	json_parse(self->json, &in, &self->data->data);
	self->lex->pos = self->json->pos;
}

hot static void
parse_row_list(Stmt* self, AstInsert* stmt, Ast* list)
{
	auto def = table_def(stmt->target->table);

	// (
	if (! stmt_if(self, '('))
		error("expected '('");

	// set next serial value
	uint64_t serial = serial_next(&stmt->target->table->serial);

	// [
	auto data = &self->data->data;
	encode_array(data);

	// value, ...
	auto column = def->column;
	for (; column; column = column->next)
	{
		auto offset = code_data_offset(self->data);
		if (list && list->column->order == column->order)
		{
			// parse and encode json value
			parse_value(self);

			// ,
			list = list->next;
			if (list) {
				if (! stmt_if(self, ','))
					error("row has incorrect number of columns");
			}

		} else
		{
			// GENERATED
			auto cons = &column->constraint;
			if (cons->generated == GENERATED_SERIAL)
			{
				encode_integer(data, serial);
			} else
			if (cons->generated == GENERATED_RANDOM)
			{
				auto value = random_generate(global()->random) % cons->modulo;
				encode_integer(data, value);
			} else
			{
				// use DEFAULT (NULL by default)
				buf_write(data, cons->value.start, buf_size(&cons->value));
			}
		}

		// ensure NOT NULL constraint
		if (column->constraint.not_null)
		{
			if (data_is_null(code_data_at(self->data, offset)))
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));
		}
	}

	// ]
	encode_array_end(data);

	// )
	if (! stmt_if(self, ')'))
		error("expected ')'");
}

hot static inline void
parse_row(Stmt* self, AstInsert* stmt)
{
	auto def = table_def(stmt->target->table);

	// (
	if (! stmt_if(self, '('))
		error("expected '('");

	// [
	auto data = &self->data->data;
	encode_array(data);

	// value, ...
	auto column = def->column;
	for (; column; column = column->next)
	{
		// parse and encode json value
		auto offset = code_data_offset(self->data);
		parse_value(self);

		// ensure NOT NULL constraint
		if (column->constraint.not_null)
		{
			if (data_is_null(code_data_at(self->data, offset)))
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));
		}

		// ,
		if (stmt_if(self, ','))
		{
			if (! column->next)
				error("row has incorrect number of columns");
			continue;
		}
	}

	// ]
	encode_array_end(data);

	// )
	if (! stmt_if(self, ')'))
		error("expected ')'");
}

hot static inline void
parse_values(Stmt* self, AstInsert* stmt, bool list_in_use, Ast* list)
{
	// [VALUES]
	if (stmt_if(self, KVALUES))
	{
		// (value, ...), ...
		for (;;)
		{
			if (list_in_use)
				parse_row_list(self, stmt, list);
			else
				parse_row(self, stmt);
			if (! stmt_if(self, ','))
				break;
		}
		return;
	}

	// one column case
	auto def = table_def(stmt->target->table);
	if (unlikely(list || def->column_count > 1))
		error("INSERT INTO <VALUES> expected");

	// no constraints applied
	auto data = &self->data->data;

	// value, ...
	for (;;)
	{
		// [
		encode_array(data);

		// parse and encode json value
		parse_value(self);

		// ]
		encode_array_end(data);

		if (! stmt_if(self, ','))
			break;
	}
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
parse_generate(Stmt* self, AstInsert* stmt)
{
	// insert into () values (), ...

	// GENERATE count
	auto count = stmt_if(self, KINT);
	if (! count)
		error("GENERATE <count> expected");

	auto data = &self->data->data;
	auto def = table_def(stmt->target->table);
	for (uint64_t i = 0; i < count->integer; i++)
	{
		// set next serial value
		uint64_t serial = serial_next(&stmt->target->table->serial);

		// [
		encode_array(data);

		// value, ...
		auto column = def->column;
		for (; column; column = column->next)
		{
			// GENERATED
			auto cons = &column->constraint;
			if (cons->generated == GENERATED_SERIAL)
			{
				encode_integer(data, serial);
			} else
			if (cons->generated == GENERATED_RANDOM)
			{
				auto value = random_generate(global()->random) % cons->modulo;
				encode_integer(data, value);
			} else
			{
				// ensure NOT NULL constraint (for DEFAULT value)
				if (cons->not_null)
				{
					if (data_is_null(cons->value.start))
						error("column <%.*s> value cannot be NULL", str_size(&column->name),
						      str_of(&column->name));
				}

				// use DEFAULT (NULL by default)
				buf_write(data, cons->value.start, buf_size(&cons->value));
			}
		}

		// ]
		encode_array_end(data);
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

	// [
	stmt->rows = code_data_offset(self->data);
	auto data = &self->data->data;
	encode_array(data);

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

	// ]
	encode_array_end(data);

	// ON CONFLICT
	parse_on_conflict(self, stmt);
}
