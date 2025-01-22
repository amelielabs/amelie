
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

void
json_init(Json* self)
{
	self->pos = NULL;
	self->end = NULL;
	self->buf = NULL;
	buf_init(&self->buf_data);
	buf_init(&self->stack);
}

void
json_free(Json* self)
{
	buf_free(&self->buf_data);
	buf_free(&self->stack);
}

void
json_reset(Json* self)
{
	self->pos = NULL;
	self->end = NULL;
	self->buf = NULL;
	buf_reset(&self->buf_data);
	buf_reset(&self->stack);
}

typedef enum
{
	JSON_VALUE,
	JSON_ARRAY_NEXT,
	JSON_MAP_KEY,
	JSON_MAP_NEXT
} JsonState;

hot static inline void
json_push(Json* self, JsonState state)
{
	buf_write(&self->stack, &state, sizeof(state));
}

hot static inline JsonState
json_pop(Json* self)
{
	if (unlikely(buf_empty(&self->stack)))
		error("unexpected end of JSON object");
	buf_truncate(&self->stack, sizeof(JsonState));
	return *(JsonState*)self->stack.position;
}

hot static inline uint8_t
json_next(Json* self)
{
	while (self->pos < self->end && isspace(*self->pos))
		self->pos++;
	if (unlikely(self->pos == self->end))
		error("unexpected end of line");
	return *self->pos;
}

hot static inline bool
json_is_keyword(Json* self, const char* name, int name_size)
{
	return (self->pos + name_size) <= self->end &&
	        !strncasecmp(self->pos, name, name_size);
}

hot static inline void
json_keyword(Json* self, const char* name, int name_size)
{
	if (unlikely(! json_is_keyword(self, name, name_size)))
		error("unexpected JSON token");
	self->pos += name_size;
}

hot static inline void
json_string_read(Json* self, Str* str, int string_end)
{
	// "
	self->pos++;
	auto start = self->pos;
	bool slash = false;
	while (self->pos < self->end)
	{
		if (*self->pos == string_end) {
			if (slash) {
				slash = false;
				self->pos++;
				continue;
			}
			break;
		}
		if (unlikely(*self->pos == '\n'))
			error("unterminated JSON string");
		if (*self->pos == '\\') {
			slash = !slash;
		} else {
			slash = false;
		}
		if (unlikely(utf8_next(&self->pos, self->end) == -1))
			error("invalid JSON string UTF8 encoding");
	}
	if (unlikely(self->pos == self->end))
		error("unterminated JSON string");
	auto size = self->pos - start;
	self->pos++;
	str_set(str, start, size);
}

hot static inline bool
json_string(Json* self, Str* str)
{
	auto next = json_next(self);
	if (next != '\"' && next != '\'')
		return false;
	json_string_read(self, str, next);
	return true;
}

hot static inline bool
json_integer_read(Json* self, int64_t* value, double* real)
{
	bool minus = false;
	if (*self->pos == '-')
	{
		minus = true;
		self->pos++;
	}
	auto start = self->pos;
	while (self->pos < self->end)
	{
		if (*self->pos == '.' ||
		    *self->pos == 'e' ||
		    *self->pos == 'E')
		{
			self->pos = start;
			goto read_as_double;
		}
		if (! isdigit(*self->pos))
			break;
		if (unlikely(int64_mul_add_overflow(value, *value, 10, *self->pos - '0')))
			error("JSON int overflow");
		self->pos++;
	}
	if (minus)
		*value = -(*value);
	return true;

read_as_double:
	errno = 0;
	char* end = NULL;
	*real = strtod(start, &end);
	if (errno == ERANGE)
		error("bad JSON float number");
	self->pos = end;
	if (minus)
		*real = -(*real);
	return false;
}

hot static inline void
json_integer(Json* self)
{
	int64_t value = 0;
	double  value_real = 0;
	if (json_integer_read(self, &value, &value_real))
		encode_integer(self->buf, value);
	else
		encode_real(self->buf, value_real);
}

hot static inline void
json_const(Json* self)
{
	switch (*self->pos) {
	case '"':
	case '\'':
	{
		Str str;
		json_string_read(self, &str, *self->pos);
		encode_string(self->buf, &str);
		return;
	}
	case 'n':
	case 'N':
	{
		json_keyword(self, "null", 4);
		encode_null(self->buf);
		return;
	}
	case 't':
	case 'T':
	{
		json_keyword(self, "true", 4);
		encode_bool(self->buf, true);
		return;
	}
	case 'f':
	case 'F':
	{
		json_keyword(self, "false", 5);
		encode_bool(self->buf, false);
		return;
	}
	default:
		if (likely(isdigit(*self->pos) || *self->pos == '.' || *self->pos == '-'))
		{
			json_integer(self);
			return;
		}
		break;
	}
	error("unexpected JSON token");
}

hot void
json_parse(Json* self, Str* text, Buf* buf)
{
	self->pos = str_of(text);
	self->end = str_of(text) + str_size(text);
	if (buf == NULL)
	{
		self->buf = &self->buf_data;
		buf = self->buf;
	} else
	{
		self->buf = buf;
	}

	// array  ::= [ [value] [, ...] ]
	// obj    ::= { ["key": value] [, ...] }
	// const  ::= int | real  | bool | string | null
	// value  ::= array | obj | const

	// push expr
	//
	// while has stack
	//
	//    pop = stack()
	//    switch pop
	//      elem:
	//         if [
	//            if ]
	//              write []
	//            push array_next
	//            push elem
	//         if {, push obj_key
	//            if }
	//              write {}
	//            push obj_key
	//         if const, push const
	//         or error
	//
	//      array_next:
	//         if ,
	//            push array_next
	//            push elem
	//         if ], end
	//         or error
	//
	//      obj_key:
	//        if not "key", error
	//        if not ":", error
	//        push obj_next
	//        push elem
	//
	//      obj_next:
	//         if ,
	//            push obj_key
	//         if }, end
	//         or error
	//

	json_push(self, JSON_VALUE);

	while (! buf_empty(&self->stack))
	{
		auto next = json_next(self);
		auto term = json_pop(self);
		switch (term) {
		case JSON_VALUE:
		{
			if (next == '[')
			{
				encode_array(buf);
				self->pos++;

				// []
				next = json_next(self);
				if (next == ']')
				{
					encode_array_end(buf);
					self->pos++;
					break;
				}

				// [value, ...]
				json_push(self, JSON_ARRAY_NEXT);
				json_push(self, JSON_VALUE);
			} else
			if (next == '{')
			{
				encode_obj(buf);
				self->pos++;

				// {}
				next = json_next(self);
				if (next == '}')
				{
					encode_obj_end(buf);
					self->pos++;
					break;
				}

				// {"key": value}
				json_push(self, JSON_MAP_KEY);
			} else
			{
				json_const(self);
			}
			break;
		}
		case JSON_ARRAY_NEXT:
		{
			if (next == ',')
			{
				json_push(self, JSON_ARRAY_NEXT);
				json_push(self, JSON_VALUE);
				self->pos++;
				break;
			}
			if (next == ']')
			{
				encode_array_end(buf);
				self->pos++;
				break;
			}
			error("unexpected JSON array token");
			break;
		}
		case JSON_MAP_KEY:
		{
			// "key"
			Str str;
			if (! json_string(self, &str))
				error("JSON object key expected");
			encode_string(self->buf, &str);
			// ':'
			next = json_next(self);
			if (next != ':')
				error("unexpected JSON object token");
			self->pos++;
			// value
			json_push(self, JSON_MAP_NEXT);
			json_push(self, JSON_VALUE);
			break;
		}
		case JSON_MAP_NEXT:
		{
			if (next == ',')
			{
				json_push(self, JSON_MAP_KEY);
				self->pos++;
				break;
			}
			if (next == '}')
			{
				encode_obj_end(buf);
				self->pos++;
				break;
			}
			error("unexpected JSON object token");
			break;
		}
		}
	}
}
