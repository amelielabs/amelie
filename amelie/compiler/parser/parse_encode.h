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
	// time-related values
	case KINTERVAL:
		encode_string(buf, &self->string);
		break;
	case KTIMESTAMP:
		encode_string(buf, &self->string);
		break;
	case KCURRENT_TIMESTAMP:
	{
		auto offset = buf_size(buf);
		encode_string32(buf, 0);
		buf_reserve(buf, 128);
		int size = timestamp_write(self->integer, local->timezone, (char*)buf->position, 128);
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
