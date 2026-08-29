
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
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

hot int
parse_vector(Stmt* self, Buf* buf)
{
	// [
	stmt_expect(self, '[');

	// []
	if (stmt_if(self, ']'))
		return 0;

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
		if (ast->id == KDECIMAL)
			value = (float)decimal_get_double(ast->decimal);
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

	return count;
}

hot static int
parse_vector_decode(Buf* buf, uint8_t** pos)
{
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
	return count;
}

hot Ast*
parse_value_const(Stmt* self, Column* column, Value* value)
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
		int minus = 1;
		if (ast->id == '-')
		{
			minus = -1;
			ast = stmt_next(self);
		}
		if (likely(ast->id == KINT))
			value_set_int(value, ast->integer * minus);
		else
		if (ast->id == KDECIMAL)
			value_set_int(value, decimal_get_int(ast->decimal) * minus);
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
		if (ast->id == KDECIMAL)
			value_set_double(value, decimal_get_double(ast->decimal) * minus);
		else
		if (ast->id == KREAL)
			value_set_double(value, ast->real * minus);
		else
			break;
		return ast;
	}
	case TYPE_DECIMAL:
	{
		auto     cons = &column->constraints;
		uint64_t decimal;
		if (likely(ast->id == KDECIMAL))
			// decimal value will be converted during row creation
			decimal = ast->decimal;
		else
		if (ast->id == KINT)
			decimal = decimal_set_int(cons->decimal, cons->decimal_scale, ast->integer);
		else
		if (ast->id == KREAL)
			decimal = decimal_set_double_round(cons->decimal, cons->decimal_scale, ast->real);
		else
			break;
		value_set_decimal(value, decimal);
		return ast;
	}
	case TYPE_DATE:
	{
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
	case TYPE_TIMESTAMP:
	{
		// unixtime
		if (ast->id == KINT) {
			value_set_timestamp(value, ast->integer);
			return ast;
		}

		// [TIMESTAMP] string
		if (ast->id == KTIMESTAMP)
			ast = stmt_expect(self, KSTRING);

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
			ast = stmt_expect(self, KSTRING);

		Interval iv;
		interval_init(&iv);
		if (unlikely(error_catch( interval_set(&iv, &ast->string) )))
			stmt_error(self, ast, "invalid interval value");
		value_set_interval(value, &iv);
		return ast;
	}
	case TYPE_UUID:
	{
		// [UUID] string
		if (ast->id == KUUID)
			ast = stmt_expect(self, KSTRING);

		Uuid uuid;
		uuid_init(&uuid);
		if (uuid_set_nothrow(&uuid, &ast->string) == -1)
			stmt_error(self, ast, "invalid uuid value");
		value_set_uuid(value, &uuid);
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
	case TYPE_VECTOR:
	{
		// [VECTOR] [array]
		if (ast->id == KVECTOR)
			ast = stmt_next(self);
		stmt_push(self, ast);
		auto buf = buf_create();
		errdefer_buf(buf);
		auto dim = parse_vector(self, buf);
		if (dim != (column->size_flat / sizeof(float)))
			stmt_error(self, ast, "invalid vector dimension");
		value_set_vector_buf(value, dim, buf);
		return ast;
	}
	}

	stmt_error(self, ast, "'{s}' expected for column '{str}'",
	           type_of(column->type), &column->name);
	return NULL;
}

hot Ast*
parse_value(Stmt* self, From* from, Column* column, Value* value)
{
	// handle as reference pushdown (execute before send)
	auto ast = parse_expr(self, NULL);
	if (! parse_expr_is_const(ast))
	{
		// vector, { or [ with expressions
		auto ref = refs_add(&self->refs, from, ast, -1);
		ref->not_null = column->constraints.not_null;
		ref->column   = column;
		value_set_ref(value, ref->order);

		// do early vector dimension check
		if (column->type == TYPE_VECTOR && ast->id == KVECTOR)
		{
			int dim = (column->size_flat / sizeof(float));
			if (ast->vector_dim != dim)
				stmt_error(self, ast, "invalid vector dimension");
		}
		return ast;
	}

	// const path
	if (ast->id == KNULL)
	{
		value_set_null(value);
		return ast;
	}

	// result of compile time function execution
	if (ast->id == KVALUE)
	{
		auto result = set_value(ast->set, 0);
		if (result->type == TYPE_NULL)
		{
			value_set_null(value);
			return ast;
		}
		if (result->type != column->type)
			stmt_error(self, ast, "expected '{s}'", type_of(column->type));
		value_copy(value, result);
		return ast;
	}

	switch (column->type) {
	case TYPE_BOOL:
	{
		if (unlikely(ast->id != KTRUE && ast->id != KFALSE))
			break;
		value_set_bool(value, ast->id == KTRUE);
		return ast;
	}
	case TYPE_INT:
	{
		if (likely(ast->id == KINT))
			value_set_int(value, ast->integer);
		else
		if (ast->id == KDECIMAL)
			value_set_int(value, decimal_get_int(ast->decimal));
		else
		if (ast->id == KREAL)
			value_set_int(value, ast->real);
		else
			break;
		return ast;
	}
	case TYPE_DOUBLE:
	{
		if (likely(ast->id == KREAL))
			value_set_double(value, ast->real);
		else
		if (ast->id == KDECIMAL)
			value_set_double(value, decimal_get_double(ast->decimal));
		else
		if (ast->id == KINT)
			value_set_double(value, ast->integer);
		else
			break;
		return ast;
	}
	case TYPE_DECIMAL:
	{
		auto     cons = &column->constraints;
		uint64_t decimal;
		if (likely(ast->id == KDECIMAL))
			// decimal value will be converted during row creation
			decimal = ast->decimal;
		else
		if (ast->id == KINT)
			decimal = decimal_set_int(cons->decimal, cons->decimal_scale, ast->integer);
		else
		if (ast->id == KREAL)
			decimal = decimal_set_double_round(cons->decimal, cons->decimal_scale, ast->real);
		else
			break;
		value_set_decimal(value, decimal);
		return ast;
	}
	case TYPE_DATE:
	{
		// [DATE] string
		if (unlikely(ast->id != KDATE && ast->id != KSTRING))
			break;
		int julian;
		if (unlikely(error_catch( julian = date_set(&ast->string) )))
			stmt_error(self, ast, "invalid date value");
		value_set_date(value, julian);
		return ast;
	}
	case TYPE_TIMESTAMP:
	{
		// unixtime
		if (ast->id == KINT) {
			value_set_timestamp(value, ast->integer);
			return ast;
		}

		// [TIMESTAMP] string
		if (unlikely(ast->id != KTIMESTAMP && ast->id != KSTRING))
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
		if (unlikely(ast->id != KINTERVAL && ast->id != KSTRING))
			break;
		Interval iv;
		interval_init(&iv);
		if (unlikely(error_catch( interval_set(&iv, &ast->string) )))
			stmt_error(self, ast, "invalid interval value");
		value_set_interval(value, &iv);
		return ast;
	}
	case TYPE_UUID:
	{
		// [UUID] string
		if (unlikely(ast->id != KUUID && ast->id != KSTRING))
			break;
		Uuid uuid;
		uuid_init(&uuid);
		if (uuid_set_nothrow(&uuid, &ast->string) == -1)
			stmt_error(self, ast, "invalid uuid value");
		value_set_uuid(value, &uuid);
		return ast;
	}
	case TYPE_STRING:
	{
		if (unlikely(ast->id != KSTRING))
			break;
		value_set_string(value, &ast->string, NULL);
		return ast;
	}
	case TYPE_JSON:
	{
		auto buf = buf_create();
		errdefer_buf(buf);
		ast_encode(ast, &self->parser->lex, self->parser->local, buf);
		value_set_json_buf(value, buf);
		return ast;
	}
	case TYPE_VECTOR:
	{
		// expected explicit VECTOR [...]
		break;
	}
	}

	stmt_error(self, ast, "expected '{s}'", type_of(column->type));
	return NULL;
}

hot void
parse_value_data(Local* local, Column* column, Value* value, uint8_t** pos)
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

		if (data_is_decimal(*pos))
		{
			uint64_t ref;
			unpack_decimal(pos, &ref);
			value_set_int(value, decimal_get_int(ref));
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
		if (data_is_real(*pos))
		{
			double ref;
			unpack_real(pos, &ref);
			value_set_double(value, ref);
			return;
		}

		if (data_is_decimal(*pos))
		{
			uint64_t ref;
			unpack_decimal(pos, &ref);
			value_set_double(value, decimal_get_double(ref));
			return;
		}

		if (data_is_int(*pos))
		{
			int64_t ref;
			unpack_int(pos, &ref);
			value_set_double(value, ref);
			return;
		}

		break;
	}
	case TYPE_DECIMAL:
	{
		auto     cons = &column->constraints;
		uint64_t decimal;
		if (data_is_decimal(*pos))
		{
			// decimal value will be converted during row creation
			unpack_decimal(pos, &decimal);
		} else
		if (data_is_int(*pos))
		{
			int64_t ref;
			unpack_int(pos, &ref);
			decimal = decimal_set_int(cons->decimal, cons->decimal_scale, ref);
		} else
		if (data_is_real(*pos))
		{
			double ref;
			unpack_real(pos, &ref);
			decimal = decimal_set_double_round(cons->decimal, cons->decimal_scale, ref);
		} else
			break;
		value_set_decimal(value, decimal);
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
	case TYPE_VECTOR:
	{
		if (! data_is_array(*pos))
			break;
		auto buf = buf_create();
		errdefer_buf(buf);
		auto dim = parse_vector_decode(buf, pos);
		if (dim != (column->size_flat / sizeof(float)))
			error("invalid vector dimension");
		value_set_vector_buf(value, dim, buf);
		return;
	}
	}

	error("'{s}' expected for column '{str}'", type_of(column->type),
	      &column->name);
}

hot void
parse_value_string(Local* local, Column* column, Value* value, Str* str)
{
	// null
	if (str_empty(str))
	{
		value_set_null(value);
		return;
	}

	// validate column type and set value
	switch (column->type) {
	case TYPE_BOOL:
	{
		bool ref;
		if (str_is_case(str, "true", 4))
			ref = true;
		else
		if (str_is_case(str, "false", 5))
			ref = false;
		else
		if (str_is_case(str, "1", 0))
			ref = true;
		else
		if (str_is_case(str, "0", 1))
			ref = false;
		else
			break;
		value_set_bool(value, ref);
		return;
	}
	case TYPE_INT:
	{
		int64_t ref;
		if (unlikely(str_i64(str, &ref) == -1))
			break;
		value_set_int(value, ref);
		return;
	}
	case TYPE_DOUBLE:
	{
		// TODO:
		break;
	}
	case TYPE_DECIMAL:
	{
		bool ok;
		auto decimal = decimal_set_str_nothrow(str, &ok);
		if (unlikely(! ok))
			break;
		value_set_decimal(value, decimal);
		return;
	}
	case TYPE_DATE:
	{
		int julian;
		if (unlikely(error_catch( julian = date_set(str) )))
			break;
		value_set_date(value, julian);
		return;
	}
	case TYPE_TIMESTAMP:
	{
		Timestamp ts;
		timestamp_init(&ts);
		if (unlikely(error_catch( timestamp_set(&ts, str) )))
			break;
		value_set_timestamp(value, timestamp_get_unixtime(&ts, local->timezone));
		return;
	}
	case TYPE_INTERVAL:
	{
		Interval iv;
		interval_init(&iv);
		if (unlikely(error_catch( interval_set(&iv, str) )))
			break;
		value_set_interval(value, &iv);
		return;
	}
	case TYPE_UUID:
	{
		Uuid uuid;
		uuid_init(&uuid);
		if (uuid_set_nothrow(&uuid, str) == -1)
			break;
		value_set_uuid(value, &uuid);
		return;
	}
	case TYPE_STRING:
	{
		value_set_string(value, str, NULL);
		return;
	}
	case TYPE_JSON:
	{
		Json json;
		json_init(&json);
		defer(json_free, &json);
		auto buf = buf_create();
		errdefer_buf(buf);
		json_parse(&json, str, buf);
		value_set_json_buf(value, buf);
		return;
	}
	case TYPE_VECTOR:
	{
		// TODO:
		break;
	}
	}

	error("'{s}' expected for column '{str}'", type_of(column->type),
	      &column->name);
}

hot void
parse_value_default(Column* column, Value* column_value)
{
	// IDENTITY or DEFAULT
	auto cons = &column->constraints;
	if (cons->identity)
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
	if (cons->not_null && !cons->identity)
	{
		if (self)
			stmt_error(self, expr, "column '{str}' value cannot be NULL",
			           &column->name);
		else
			error("column '{str}' value cannot be NULL",
			      &column->name);
	}
}
