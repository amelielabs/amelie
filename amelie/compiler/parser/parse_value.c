
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

hot void
parse_vector_raw(Stmt* self, Buf* buf, Str* field)
{
	(void)self;
	(void)buf;
	(void)field;
	// todo:
}

hot void
parse_vector(Stmt* self, Buf* buf)
{
	// [
	stmt_expect(self, '[');
	auto offset = buf_size(buf);
	buf_write_i32(buf, 0);

	// []
	if (stmt_if(self, ']'))
		return;

	// [float [, ...]]
	int count = 0;
	for (;;)
	{
		auto ast = stmt_next(self);

		// int or float
		float value = 0;
		if (likely(ast->id == KINT))
			value = ast->integer;
		else
		if (ast->id == KREAL)
			value = ast->real;
		else
			stmt_error(self, ast, "invalid vector value");
		buf_write_float(buf, value);
		count++;

		// ,
		ast = stmt_next(self);
		if (ast->id == ',')
			continue;
		if (ast->id == ']')
			break;
		stmt_error(self, ast, "vector array syntax error");
	}

	*(uint32_t*)(buf->start + offset) = count;
}

hot void
parse_value_raw(Stmt* self, Column* column, Value* value, Buf* text)
{
	Str field;
	buf_str(text, &field);
	if (column->type == TYPE_STRING)
	{
		value_set_string(value, &field, text);
		return;
	}
	defer_buf(text);

	if (buf_empty(text))
	{
		value_set_null(value);
		return;
	}

	str_shrink(&field);
	switch (column->type) {
	case TYPE_BOOL:
	{
		if (str_is_case(&field, "true", 4)) {
			value_set_bool(value, true);
			return;
		}
		if (str_is_case(&field, "false", 5)) {
			value_set_bool(value, false);
			return;
		}
		break;
	}
	case TYPE_INT:
	{
		// -
		auto minus = *str_of(&field) == '-';
		if (minus)
			field.pos++;
		int64_t val;
		if (str_toint(&field, &val) == -1)
			break;
		if (minus)
			val = -val;
		value_set_int(value, val);
		return;
	}
	case TYPE_DOUBLE:
	{
		// -
		auto minus = *str_of(&field) == '-';
		if (minus)
			field.pos++;
		double val;
		if (str_todouble(&field, &val) == -1)
			break;
		if (minus)
			val = -val;
		value_set_double(value, val);
		return;
	}
	case TYPE_JSON:
	{
		// parse and encode json value
		auto buf = buf_create();
		errdefer_buf(buf);
		json_parse(self->json, &field, buf);
		value_set_json_buf(value, buf);
		return;
	}
	case TYPE_TIMESTAMP:
	{
		Timestamp ts;
		timestamp_init(&ts);
		if (unlikely(error_catch( timestamp_set(&ts, &field) )))
			stmt_error(self, NULL, "invalid timestamp value");
		value_set_timestamp(value, timestamp_get_unixtime(&ts, self->local->timezone));
		return;
	}
	case TYPE_INTERVAL:
	{
		Interval iv;
		interval_init(&iv);
		if (unlikely(error_catch( interval_set(&iv, &field) )))
			stmt_error(self, NULL, "invalid interval value");
		value_set_interval(value, &iv);
		return;
	}
	case TYPE_DATE:
	{
		int julian;
		if (unlikely(error_catch( julian = date_set(&field) )))
			stmt_error(self, NULL, "invalid date value");
		value_set_date(value, julian);
		return;
	}
	case TYPE_VECTOR:
	{
		auto buf = buf_create();
		errdefer_buf(buf);
		parse_vector_raw(self, buf, &field);
		value_set_vector_buf(value, buf);
		return;
	}
	case TYPE_UUID:
	{
		Uuid uuid;
		uuid_init(&uuid);
		if (uuid_set_nothrow(&uuid, &field) == -1)
			stmt_error(self, NULL, "invalid uuid value");
		value_set_uuid(value, &uuid);
		return;
	}
	}

	stmt_error(self, NULL, "'%s' expected for column '%.*s'",
	           type_of(column->type),
	           str_size(&column->name), str_of(&column->name));
}

hot Ast*
parse_value(Stmt* self, Column* column, Value* value)
{
	auto ast = stmt_next(self);
	if (ast->id == KNULL)
	{
		value_set_null(value);
		return ast;
	}
	switch (column->type) {
	case TYPE_BOOL:
		if (ast->id != KTRUE && ast->id != KFALSE)
			break;
		value_set_bool(value, ast->id == KTRUE);
		return ast;
	case TYPE_INT:
	{
		if (likely(ast->id == KINT))
			value_set_int(value, ast->integer);
		else
		if (ast->id == KREAL)
			value_set_int(value, ast->real);
		else
			break;
		return ast;
	}
	case TYPE_DOUBLE:
	{
		if (likely(ast->id == KINT))
			value_set_double(value, ast->integer);
		else
		if (ast->id == KREAL)
			value_set_double(value, ast->real);
		else
			break;
		return ast;
	}
	case TYPE_STRING:
	{
		if (likely(ast->id != KSTRING))
			break;
		value_set_string(value, &ast->string, NULL);
		return ast;
	}
	case TYPE_JSON:
	{
		// parse and encode json value
		auto lex = self->lex;
		lex_push(lex, ast);
		auto pos = lex->pos;
		while (lex->backlog)
		{
			pos = lex->start + lex->backlog->pos_start;
			lex->backlog = lex->backlog->prev;
		}
		json_reset(self->json);
		Str in;
		str_set(&in, pos, lex->end - pos);
		auto buf = buf_create();
		errdefer_buf(buf);
		json_parse(self->json, &in, buf);
		lex->pos = self->json->pos;
		value_set_json_buf(value, buf);
		return ast;
	}
	case TYPE_TIMESTAMP:
	{
		// current_timestamp
		if (ast->id == KCURRENT_TIMESTAMP) {
			value_set_timestamp(value, self->local->time_us);
			return ast;
		}

		// unixtime
		if (ast->id == KINT) {
			value_set_timestamp(value, ast->integer);
			return ast;
		}

		// [TIMESTAMP] string
		if (ast->id == KTIMESTAMP)
			ast = stmt_next(self);
		if (likely(ast->id != KSTRING))
			break;

		Timestamp ts;
		timestamp_init(&ts);
		if (unlikely(error_catch( timestamp_set(&ts, &ast->string) )))
			stmt_error(self, ast, "invalid timestamp value");
		value_set_timestamp(value, timestamp_get_unixtime(&ts, self->local->timezone));
		return ast;
	}
	case TYPE_INTERVAL:
	{
		// [INTERVAL] string
		if (ast->id == KINTERVAL)
			ast = stmt_next(self);
		if (likely(ast->id != KSTRING))
			break;
		Interval iv;
		interval_init(&iv);
		if (unlikely(error_catch( interval_set(&iv, &ast->string) )))
			stmt_error(self, ast, "invalid interval value");
		value_set_interval(value, &iv);
		return ast;
	}
	case TYPE_DATE:
	{
		// current_date
		if (ast->id == KCURRENT_DATE) {
			value_set_date(value, timestamp_date(self->local->time_us));
			return ast;
		}

		// [DATE] string
		if (ast->id == KDATE)
			ast = stmt_next(self);
		if (likely(ast->id != KSTRING))
			break;

		int julian;
		if (unlikely(error_catch( julian = date_set(&ast->string) )))
			stmt_error(self, ast, "invalid date value");
		value_set_date(value, julian);
		return ast;
	}
	case TYPE_VECTOR:
	{
		// [VECTOR] [array]
		if (ast->id == KVECTOR)
			ast = stmt_next(self);
		stmt_push(self, ast);
		auto buf = buf_create();
		errdefer_buf(buf);
		parse_vector(self, buf);
		value_set_vector_buf(value, buf);
		return ast;
	}
	case TYPE_UUID:
	{
		// [UUID] string
		if (ast->id == KUUID)
			ast = stmt_next(self);
		if (likely(ast->id != KSTRING))
			break;
		Uuid uuid;
		uuid_init(&uuid);
		if (uuid_set_nothrow(&uuid, &ast->string) == -1)
			stmt_error(self, ast, "invalid uuid value");
		value_set_uuid(value, &uuid);
		return ast;
	}
	}

	stmt_error(self, ast, "'%s' expected for column '%.*s'",
	           type_of(column->type),
	           str_size(&column->name), str_of(&column->name));
	return NULL;
}

hot void
parse_value_default(Stmt*    self,
                    Column*  column,
                    Value*   column_value,
                    uint64_t seq)
{
	// IDENTITY, RANDOM or DEFAULT
	unused(self);
	auto cons = &column->constraints;
	if (cons->as_identity)
	{
		value_set_int(column_value, seq);
	} else
	if (cons->random)
	{
		auto value = random_generate(global()->random) % cons->random_modulo;
		value_set_int(column_value, value);
	} else
	{
		value_decode(column_value, cons->value.start, NULL);
	}
}

void
parse_value_validate(Stmt* self, Column* column, Value* column_value, Ast* expr)
{
	// ensure NOT NULL constraint
	if (column_value->type == TYPE_NULL)
	{
		// value can be NULL for generated column (will be rechecked later)
		if (! str_empty(&column->constraints.as_stored))
			return;

		if (column->constraints.not_null)
			stmt_error(self, expr, "column '%.*s' value cannot be NULL",
			           str_size(&column->name),
			           str_of(&column->name));
	}
}
