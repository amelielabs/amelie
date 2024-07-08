
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>

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
json_parse_keyword(Json* self, const char* name, int name_size)
{
	if (unlikely(! json_is_keyword(self, name, name_size)))
		error("unexpected token");
	self->pos += name_size;
}

hot static inline void
json_cast(Json* self, Str* str)
{
	// ::timestamp
	if (json_is_keyword(self, "::timestamp", 11))
	{
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read(&ts, str);
		encode_timestamp(self->buf, timestamp_of(&ts, false));
		self->pos += 11;
		return;
	}

	// ::timestamptz
	if (json_is_keyword(self, "::timestamptz", 13))
	{
		// todo: pass tzz
		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read(&ts, str);
		encode_timestamp(self->buf, timestamp_of(&ts, true));
		self->pos += 13;
		return;
	}

	// ::interval
	if (json_is_keyword(self, "::interval", 10))
	{
		Interval iv;
		interval_init(&iv);
		interval_read(&iv, str);
		encode_interval(self->buf, &iv);
		self->pos += 10;
		return;
	}

	error("unrecognized :: cast operator");
}

hot static inline void
json_parse_string(Json* self, bool cast)
{
	// "
	self->pos++;

	auto start = self->pos;
	bool slash = false;
	while (self->pos < self->end)
	{
		if (*self->pos == '\"') {
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

	// ::<operation>
	if (cast && self->pos < self->end && *self->pos == ':')
	{
		Str str;
		str_set(&str, start, size);
		json_cast(self, &str);
	} else
	{
		encode_raw(self->buf, start, size);
	}
}

hot static inline void
json_parse_integer(Json* self)
{
	bool minus = false;
	if (*self->pos == '-')
	{
		minus = true;
		self->pos++;
	}

	int64_t value = 0;
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
		value = (value * 10) + *self->pos - '0';
		self->pos++;
	}

	if (minus)
		value = -value;
	encode_integer(self->buf, value);
	return;

read_as_double:
	errno = 0;
	char* end = NULL;
	double real = strtod(start, &end);
	if (errno == ERANGE)
		error("bad float number token");
	self->pos = end;
	if (minus)
		real = -real;
	encode_real(self->buf, real);
}

hot static inline void
json_parse_const(Json* self)
{
	switch (*self->pos) {
	case '"':
		json_parse_string(self, true);
		return;
	case 'n':
		json_parse_keyword(self, "null", 4);
		encode_null(self->buf);
		return;
	case 't':
		json_parse_keyword(self, "true", 4);
		encode_bool(self->buf, true);
		return;
	case 'f':
		json_parse_keyword(self, "false", 5);
		encode_bool(self->buf, false);
		return;
	default:
		if (likely(isdigit(*self->pos) || *self->pos == '.' || *self->pos == '-'))
		{
			json_parse_integer(self);
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
				json_parse_const(self);
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
			if (unlikely(*self->pos != '\"'))
				error("map key expected");
			json_parse_string(self, false);
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
