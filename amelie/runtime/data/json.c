
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>

void
json_init(Json* self)
{
	self->pos = NULL;
	self->end = NULL;
	self->buf = NULL;
	self->tz  = NULL;
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
	self->tz  = NULL;
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
	        !memcmp(self->pos, name, name_size);
}

hot static inline void
json_keyword(Json* self, const char* name, int name_size)
{
	if (unlikely(! json_is_keyword(self, name, name_size)))
		error("unexpected token");
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
		error("bad float number token");
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
	{
		json_keyword(self, "null", 4);
		encode_null(self->buf);
		return;
	}
	case 't':
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
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read(&ts, &str);
			encode_timestamp(self->buf, timestamp_of(&ts, self->tz));
		} else {
			break;
		}
		return;
	}
	case 'f':
	{
		json_keyword(self, "false", 5);
		encode_bool(self->buf, false);
		return;
	}
	case 'i':
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
json_parse(Json* self, Timezone* tz, Str* text, Buf* buf)
{
	self->pos = str_of(text);
	self->end = str_of(text) + str_size(text);
	self->tz  = tz;
	if (buf == NULL)
	{
		self->buf = &self->buf_data;
		buf = self->buf;
	} else
	{
		self->buf = buf;
	}

	// array ::=  [ [obj] [, ...] ]
	// map   ::=  { ["key": obj] [, ...] }
	// const ::=  int | real  | bool | string | null
	// obj   ::=  array | map | const

	// push obj
	//
	// while has stack
	//
	//    pop = stack()
	//    switch pop
	//      obj:
	//         if [
	//            if ]
	//              write []
	//            push array_next
	//            push obj
	//         if {, push map_key
	//            if }
	//              write {}
	//            push map_key
	//         if const, push const
	//         or error
	//
	//      array_next:
	//         if ,
	//            push array_next
	//            push obj
	//         if ], end
	//         or error
	//
	//      map_key:
	//        if not "key", error
	//        if not ":", error
	//        push map_next
	//        push obj
	//
	//      map_next:
	//         if ,
	//            push map_key
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
				encode_map(buf);
				self->pos++;

				// {}
				next = json_next(self);
				if (next == '}')
				{
					encode_map_end(buf);
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
				error("map key expected");
			encode_string(self->buf, &str);
			// ':'
			next = json_next(self);
			if (next != ':')
				error("unexpected map token");
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
				encode_map_end(buf);
				self->pos++;
				break;
			}
			error("unexpected map token");
			break;
		}
		}
	}
}
