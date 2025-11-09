#pragma once

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

hot static inline void
ast_encode(Ast* self, Lex* lex, Local* local, Buf* buf)
{
	switch (self->id) {
	// const
	case KNULL:
		encode_null(buf);
		break;
	case KREAL:
		encode_real(buf, self->real);
		break;
	case KINT:
		encode_integer(buf, self->integer);
		break;
	case KSTRING:
		if (self->string_escape)
			encode_string_unescape(buf, &self->string);
		else
			encode_string(buf, &self->string);
		break;
	case KTRUE:
		encode_bool(buf, true);
		break;
	case KFALSE:
		encode_bool(buf, false);
		break;
	case KNEG:
		if (self->l->id == KINT)
			encode_integer(buf, -self->l->integer);
		else
			encode_real(buf, -self->l->real);
		break;
	case KUUID:
		encode_string(buf, &self->string);
		break;
	// time-related values
	case KINTERVAL:
	case KTIMESTAMP:
	case KDATE:
		encode_string(buf, &self->string);
		break;
	case KCURRENT_TIMESTAMP:
	{
		auto offset = buf_size(buf);
		encode_string32(buf, 0);
		buf_reserve(buf, 128);
		int size = timestamp_get(local->time_us, local->timezone, (char*)buf->position, 128);
		buf_advance(buf, size);
		uint8_t* pos = buf->start + offset;
		json_write_string32(&pos, size);
		break;
	}
	case KCURRENT_DATE:
	{
		auto offset = buf_size(buf);
		encode_string32(buf, 0);
		buf_reserve(buf, 128);
		auto julian = timestamp_date(local->time_us);
		int size = date_get(julian, (char*)buf->position, 128);
		buf_advance(buf, size);
		uint8_t* pos = buf->start + offset;
		json_write_string32(&pos, size);
		break;
	}
	// []
	case KARRAY:
	{
		if (! ast_args_of(self->l)->constable)
			lex_error(lex, self, "JSON value contains expressions");
		encode_array(buf);
		auto current = self->l->l;
		for (; current; current = current->next)
			ast_encode(current, lex, local, buf);
		encode_array_end(buf);
		break;
	}
	// {}
	case '{':
	{
		if (! ast_args_of(self->l)->constable)
			lex_error(lex, self, "JSON value contains expressions");
		encode_obj(buf);
		auto current = self->l->l;
		for (; current; current = current->next)
			ast_encode(current, lex, local, buf);
		encode_obj_end(buf);
		break;
	}

	// precomputed value
	case KVALUE:
		value_encode(set_value(self->set, 0), local->timezone, buf);
		break;

	default:
		lex_error(lex, self, "unexpected JSON value");
		break;
	}
}

hot static inline int
ast_decode(Ast* self, uint8_t* json)
{
	switch (*json) {
	case JSON_TRUE:
		self->id = KTRUE;
		break;
	case JSON_FALSE:
		self->id = KFALSE;
		break;
	case JSON_NULL:
		self->id = KNULL;
		break;
	case JSON_REAL32:
	case JSON_REAL64:
		self->id = KREAL;
		json_read_real(&json, &self->real);
		break;
	case JSON_INTV0 ... JSON_INT64:
		self->id = KINT;
		json_read_integer(&json, &self->integer);
		break;
	case JSON_STRINGV0 ... JSON_STRING32:
		self->id = KSTRING;
		json_read_string(&json, &self->string);
		break;
	case JSON_OBJ:
	case JSON_ARRAY:
	default:
		return -1;
	}
	return 0;
}

static inline void
parse_encode_value(Stmt* self, Ast* ast, Value* value)
{
	switch (ast->id) {
	// consts
	case KNULL:
		value_set_null(value);
		break;
	case KREAL:
		value_set_double(value, ast->real);
		break;
	case KINT:
		value_set_int(value, ast->integer);
		break;
	case KSTRING:
		if (ast->string_escape)
		{
			auto buf = buf_create();
			errdefer(buf_free, buf);
			unescape_str(buf, &ast->string);
			value_set_string_buf(value, buf);
		} else {
			value_set_string(value, &ast->string, NULL);
		}
		break;
	case KTRUE:
		value_set_bool(value, true);
		break;
	case KFALSE:
		value_set_bool(value, false);
		break;
	case KNEG:
		if (ast->l->id == KINT)
			value_set_int(value, -ast->l->integer);
		else
		if (ast->l->id == KREAL)
			value_set_double(value, -ast->l->real);
		else
			abort();
		break;
	case KUUID:
	{
		value_init(value);
		value->type = TYPE_UUID;
		auto uuid = &value->uuid;
		uuid_init(uuid);
		if (uuid_set_nothrow(uuid, &ast->string) == -1)
			stmt_error(self, ast, "invalid uuid value");
		break;
	}

	// time-related consts
	case KINTERVAL:
	{
		value_init(value);
		value->type = TYPE_INTERVAL;
		auto iv = &value->interval;
		interval_init(iv);
		if (unlikely(error_catch( interval_set(iv, &ast->string) )))
			stmt_error(self, ast, "invalid interval value");
		break;
	}
	case KTIMESTAMP:
	{
		Timestamp ts;
		timestamp_init(&ts);
		if (unlikely(error_catch( timestamp_set(&ts, &ast->string) )))
			stmt_error(self, ast, "invalid timestamp value");
		value_set_timestamp(value, timestamp_get_unixtime(&ts, self->parser->local->timezone));
		break;
	}
	case KDATE:
	{
		int julian;
		if (unlikely(error_catch( julian = date_set(&ast->string) )))
			stmt_error(self, ast, "invalid date value");
		value_set_date(value, julian);
		break;
	}
	case KCURRENT_TIMESTAMP:
	{
		value_set_timestamp(value, self->parser->local->time_us);
		break;
	}
	case KCURRENT_DATE:
	{
		value_set_date(value, timestamp_date(self->parser->local->time_us));
		break;
	}

	// json
	case '{':
	case KARRAY:
	{
		assert(ast_args_of(ast->l)->constable);
		auto buf = buf_create();
		errdefer(buf_free, buf);
		ast_encode(ast, &self->parser->lex, self->parser->local, buf);
		value_set_json_buf(value, buf);
		break;
	}

	// precomputed value
	case KVALUE:
		value_copy(value, set_value(ast->set, 0));
		break;

	default:
		abort();
		break;
	}
}
