
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

		// -
		auto minus = ast->id == '-';
		if (minus)
			ast = stmt_next(self);

		// int or float
		float value = 0;
		if (likely(ast->id == KINT))
			value = ast->integer;
		else
		if (ast->id == KREAL)
			value = ast->real;
		else
			stmt_error(self, ast, "invalid vector value");
		if (minus)
			value = -value;

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

hot static void
parse_vector_decode(Buf* buf, uint8_t** pos)
{
	auto offset = buf_size(buf);
	buf_write_i32(buf, 0);

	auto count = 0;
	unpack_array(pos);
	while (! unpack_array_end(pos))
	{
		double value = 0;
		if (data_is_real(*pos))
		{
			unpack_real(pos, &value);
		} else
		if (data_is_int(*pos))
		{
			int64_t ref;
			unpack_int(pos, &ref);
			value = ref;
		} else {
			error("invalid vector value");
		}
		buf_write_float(buf, (float)value);
		count++;
	}

	*(uint32_t*)(buf->start + offset) = count;
}

hot Ast*
parse_value(Stmt* self, From* from, Column* column, Value* value)
{
	auto ast = stmt_next(self);
	if (ast->id == KNULL)
	{
		value_set_null(value);
		return ast;
	}

	// variable
	if (ast->id == KNAME)
	{
		auto var = namespace_find_var(self->block->ns, &ast->string);
		if (! var)
			stmt_error(self, ast, "variable not found");

		if (var->type != column->type)
			stmt_error(self, ast, "variable does not match column '%.*s' type",
			           str_size(&column->name), str_of(&column->name));
		ast->id  = KVAR;
		ast->var = var;
		if (var->writer)
			deps_add_var(&self->deps, var->writer, var);

		auto ref = refs_add(&self->refs, from, ast, -1);
		ref->not_null = column->constraints.not_null;
		value_set_ref(value, ref->order);
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
		int minus = 1;
		if (ast->id == '-')
		{
			minus = -1;
			ast = stmt_next(self);
		}
		if (likely(ast->id == KINT))
			value_set_int(value, ast->integer * minus);
		else
		if (ast->id == KREAL)
			value_set_int(value, ast->real * minus);
		else
			break;
		return ast;
	}
	case TYPE_DOUBLE:
	{
		int minus = 1;
		if (ast->id == '-')
		{
			minus = -1;
			ast = stmt_next(self);
		}
		if (likely(ast->id == KINT))
			value_set_double(value, ast->integer * minus);
		else
		if (ast->id == KREAL)
			value_set_double(value, ast->real * minus);
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
		auto json = &self->parser->json;
		json_reset(json);
		Str in;
		str_set(&in, pos, lex->end - pos);
		auto buf = buf_create();
		errdefer_buf(buf);
		json_parse(json, &in, buf);
		lex->pos = json->pos;
		value_set_json_buf(value, buf);
		return ast;
	}
	case TYPE_TIMESTAMP:
	{
		// current_timestamp
		if (ast->id == KCURRENT_TIMESTAMP) {
			value_set_timestamp(value, self->parser->local->time_us);
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
		value_set_timestamp(value, timestamp_get_unixtime(&ts, self->parser->local->timezone));
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
			value_set_date(value, timestamp_date(self->parser->local->time_us));
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
parse_value_decode(Local* local, Column* column, Value* value, uint8_t** pos)
{
	// null
	if (data_is_null(*pos))
	{
		unpack_null(pos);
		value_set_null(value);
		return;
	}

	// validate column type and set value
	switch (column->type) {
	case TYPE_BOOL:
	{
		if (! data_is_bool(*pos))
			break;
		bool ref;
		unpack_bool(pos, &ref);
		value_set_bool(value, ref);
		return;
	}
	case TYPE_INT:
	{
		if (data_is_int(*pos))
		{
			int64_t ref;
			unpack_int(pos, &ref);
			value_set_int(value, ref);
			return;
		}

		if (data_is_real(*pos))
		{
			double ref;
			unpack_real(pos, &ref);
			value_set_int(value, (int64_t)ref);
			return;
		}
		break;
	}
	case TYPE_DOUBLE:
	{
		if (data_is_int(*pos))
		{
			int64_t ref;
			unpack_int(pos, &ref);
			value_set_double(value, ref);
			return;
		}

		if (data_is_real(*pos))
		{
			double ref;
			unpack_real(pos, &ref);
			value_set_double(value, ref);
			return;
		}
		break;
	}
	case TYPE_STRING:
	{
		if (! data_is_str(*pos))
			break;
		Str ref;
		unpack_str(pos, &ref);
		value_set_string(value, &ref, NULL);
		return;
	}
	case TYPE_JSON:
	{
		auto at = *pos;
		data_skip(pos);
		value_set_json(value, at, *pos - at, NULL);
		return;
	}
	case TYPE_TIMESTAMP:
	{
		// unixtime
		if (data_is_int(*pos))
		{
			int64_t ref;
			unpack_int(pos, &ref);
			value_set_timestamp(value, ref);
			return;
		}

		if (! data_is_str(*pos))
			break;
		Str ref;
		unpack_str(pos, &ref);

		// current_timestamp
		if (str_is_case(&ref, "current_timestamp", 17))
		{
			value_set_timestamp(value, local->time_us);
			return;
		}

		Timestamp ts;
		timestamp_init(&ts);
		if (unlikely(error_catch( timestamp_set(&ts, &ref) )))
			error("invalid timestamp value");
		value_set_timestamp(value, timestamp_get_unixtime(&ts, local->timezone));
		return;
	}
	case TYPE_INTERVAL:
	{
		if (! data_is_str(*pos))
			break;
		Str ref;
		unpack_str(pos, &ref);

		Interval iv;
		interval_init(&iv);
		if (unlikely(error_catch( interval_set(&iv, &ref) )))
			error("invalid interval value");
		value_set_interval(value, &iv);
		return;
	}

	case TYPE_DATE:
	{
		if (! data_is_str(*pos))
			break;
		Str ref;
		unpack_str(pos, &ref);

		// current_date
		if (str_is_case(&ref, "current_date", 12))
		{
			value_set_date(value, timestamp_date(local->time_us));
			return;
		}
		int julian;
		if (unlikely(error_catch( julian = date_set(&ref) )))
			error("invalid date value");
		value_set_date(value, julian);
		return;
	}

	case TYPE_VECTOR:
	{
		if (! data_is_array(*pos))
			break;
		auto buf = buf_create();
		errdefer_buf(buf);
		parse_vector_decode(buf, pos);
		value_set_vector_buf(value, buf);
		return;
	}
	case TYPE_UUID:
	{
		if (! data_is_str(*pos))
			break;
		Str ref;
		unpack_str(pos, &ref);

		Uuid uuid;
		uuid_init(&uuid);
		if (uuid_set_nothrow(&uuid, &ref) == -1)
			error("invalid uuid value");
		value_set_uuid(value, &uuid);
		return;
	}
	}

	error("'%s' expected for column '%.*s'", type_of(column->type),
	      str_size(&column->name), str_of(&column->name));
}

hot void
parse_value_default(Column* column, Value* column_value)
{
	// IDENTITY or DEFAULT
	auto cons = &column->constraints;
	if (cons->as_identity)
	{
		value_set_null(column_value);
		return;
	}

	// set default value (can be empty)
	value_data_decode(column_value, column, cons->value.start,
	                  buf_size(&cons->value));
}

void
parse_value_validate(Stmt* self, Column* column, Value* value, Ast* expr)
{
	// value can be NULL for generated column (will be rechecked later)

	// ensure NOT NULL constraint
	if (value->type != TYPE_NULL)
		return;

	auto cons = &column->constraints;
	if (! str_empty(&cons->as_stored))
		return;

	if (cons->not_null && !cons->as_identity)
	{
		if (self)
			stmt_error(self, expr, "column '%.*s' value cannot be NULL",
			           str_size(&column->name),
			           str_of(&column->name));
		else
			error("column '%.*s' value cannot be NULL",
			      str_size(&column->name),
			      str_of(&column->name));
	}
}
