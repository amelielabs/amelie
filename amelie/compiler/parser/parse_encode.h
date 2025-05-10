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
			escape_string(buf, &self->string);
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
