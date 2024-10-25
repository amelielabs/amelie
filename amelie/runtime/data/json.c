
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
#include <amelie_data.h>

void
json_init(Json* self)
{
	self->pos     = NULL;
	self->end     = NULL;
	self->buf     = NULL;
	self->tz      = NULL;
	self->time_us = 0;
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
	self->pos     = NULL;
	self->end     = NULL;
	self->buf     = NULL;
	self->time_us = 0;
	self->tz      = NULL;
	buf_reset(&self->buf_data);
	buf_reset(&self->stack);
}

void
json_set_time(Json* self, Timezone* timezone, uint64_t time)
{
	self->time_us = time;
	self->tz      = timezone;
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
		error("unexpected end of object");
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
			error("unterminated string");
		if (*self->pos == '\\') {
			slash = !slash;
		} else {
			slash = false;
		}
		self->pos++;
	}
	if (unlikely(self->pos == self->end))
		error("unterminated string");
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
		*value = (*value * 10) + *self->pos - '0';
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
		error("bad float number");
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
json_vector(Json* self)
{
	// [int| real[, ...]]
	auto next = json_next(self);
	if (next != '[')
		error("VECTOR <[> expected");
	self->pos++;

	auto buf    = self->buf;
	auto offset = buf_size(buf);
	int  count  = 0;
	buf_reserve(self->buf, data_size_vector(0));
	data_write_vector_size(&buf->position, 0);
	for (;;)
	{
		// int | real
		next = json_next(self);
		if (likely(isdigit(next) || next == '.' || next == '-'))
		{
			int64_t value_int  = 0;
			double  value_real = 0;
			float   value;
			if (json_integer_read(self, &value_int, &value_real))
				value = value_int;
			else
				value = value_real;
			buf_write(buf, &value, sizeof(float));
			count++;
			// ,
			next = json_next(self);
			if (next == ',')
			{
				self->pos++;
				continue;
			}
		}
		// ]
		if (*self->pos == ']')
		{
			self->pos++;
			break;
		}
		error("VECTOR integer of real value expected");
	}
	uint8_t* pos = buf->start + offset;
	data_write_vector_size(&pos, count);
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
		if (json_is_keyword(self, "true", 4))
		{
			json_keyword(self, "true", 4);
			encode_bool(self->buf, true);
		} else
		if (json_is_keyword(self, "timestamp", 9))
		{
			// TIMESTAMP "string"
			self->pos += 9;
			Str str;
			if (! json_string(self, &str))
				error("TIMESTAMP <string> expected");
			if (unlikely(! self->tz))
				error("unexpected operation with timestamp");
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read(&ts, &str);
			encode_timestamp(self->buf, timestamp_of(&ts, self->tz));
		} else {
			break;
		}
		return;
	}
	case 'c':
	case 'C':
	{
		if (json_is_keyword(self, "current_timestamp", 17))
		{
			auto time = self->time_us;
			if (time == 0)
				time = time_us();
			encode_timestamp(self->buf, time);
			self->pos += 17;
			return;
		}
		break;
	}
	case 'f':
	case 'F':
	{
		json_keyword(self, "false", 5);
		encode_bool(self->buf, false);
		return;
	}
	case 'i':
	case 'I':
	{
		// INTERVAL "string"
		json_keyword(self, "interval", 8);
		Str str;
		if (! json_string(self, &str))
			error("INTERVAL <string> expected");
		Interval iv;
		interval_init(&iv);
		interval_read(&iv, &str);
		encode_interval(self->buf, &iv);
		return;
	}
	case 'v':
	case 'V':
	{
		// VECTOR []
		json_keyword(self, "vector", 6);
		json_vector(self);
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
	error("unexpected token");
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
			error("unexpected array token");
			break;
		}
		case JSON_MAP_KEY:
		{
			// "key"
			Str str;
			if (! json_string(self, &str))
				error("object key expected");
			encode_string(self->buf, &str);
			// ':'
			next = json_next(self);
			if (next != ':')
				error("unexpected object token");
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
			error("unexpected object token");
			break;
		}
		}
	}
}

void
json_parse_hint(Json* self, Str* text, Buf* buf, JsonHint hint)
{
	if (hint == JSON_HINT_NONE)
	{
		json_parse(self, text, buf);
		return;
	}

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

	switch (hint) {
	case JSON_HINT_TIMESTAMP:
	{
		// timestamp or string

		// TIMESTAMP "string"
		auto is_timestamp = json_is_keyword(self, "timestamp", 9);
		if (is_timestamp)
			self->pos += 9;
		Str str;
		if (! json_string(self, &str))
		{
			if (is_timestamp)
				error("TIMESTAMP <string> expected");
			break;
		}
		if (unlikely(! self->tz))
			error("unexpected operation with timestamp");
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read(&ts, &str);
		encode_timestamp(self->buf, timestamp_of(&ts, self->tz));
		return;
	}
	case JSON_HINT_INTERVAL:
	{
		// interval or string

		// INTERVAL "string"
		auto is_interval = json_is_keyword(self, "interval", 8);
		if (is_interval)
			self->pos += 8;
		Str str;
		if (! json_string(self, &str))
		{
			if (is_interval)
				error("INTERVAL <string> expected");
			break;
		}
		Interval iv;
		interval_init(&iv);
		interval_read(&iv, &str);
		encode_interval(self->buf, &iv);
		return;
	}
	case JSON_HINT_VECTOR:
	{
		// VECTOR []
		auto is_vector = json_is_keyword(self, "vector", 6);
		if (is_vector)
			self->pos += 6;

		if (json_next(self) != '[')
		{
			if (is_vector)
				error("VECTOR <[> expected");
			break;
		}

		// []
		json_vector(self);
		return;
	}
	default:
		break;
	}

	json_parse(self, text, buf);
}
