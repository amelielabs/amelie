#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

hot static inline void
body_ensure_limit(Buf* self)
{
	auto limit = var_int_of(&config()->limit_send);
	if (unlikely((uint64_t)buf_size(self) >= limit))
		error("http reply limit reached");
}

hot static inline void
body_begin(Buf* self)
{
	buf_write(self, "[", 1);
}

hot static inline void
body_end(Buf* self)
{
	buf_write(self, "]", 1);
}

hot static inline void
body_add(Buf* self, Value* value, Timezone* timezone, bool pretty, bool wrap)
{
	// wrap body in [] unless returning array, vector, set or merge
	wrap = wrap && buf_empty(self) &&
	       (value->type != VALUE_ARRAY  &&
	        value->type != VALUE_VECTOR &&
	        value->type != VALUE_SET    &&
	        value->type != VALUE_MERGE);
	if (wrap)
		body_begin(self);

	switch (value->type) {
	case VALUE_INT:
		buf_printf(self, "%" PRIi64, value->integer);
		break;
	case VALUE_BOOL:
		if (value->integer > 0)
			buf_write(self, "true", 4);
		else
			buf_write(self, "false", 5);
		break;
	case VALUE_REAL:
		buf_printf(self, "%g", value->real);
		break;
	case VALUE_STRING:
		buf_write(self, "\"", 1);
		escape_string_raw(self, &value->string);
		buf_write(self, "\"", 1);
		break;
	case VALUE_NULL:
		buf_write(self, "null", 4);
		break;
	case VALUE_MAP:
	case VALUE_ARRAY:
	{
		uint8_t* pos = value->data;
		if (pretty)
			json_export_pretty(self, timezone, &pos);
		else
			json_export(self, timezone, &pos);
		break;
	}
	case VALUE_VECTOR:
	{
		buf_write(self, "[", 1);
		for (int i = 0; i < value->vector.size; i++)
		{
			char buf[32];
			auto buf_len = snprintf(buf, sizeof(buf), "%g%s", value->vector.value[i],
			                        i != value->vector.size - 1? ", ": "");
			buf_write(self, buf, buf_len);
		}
		buf_write(self, "]", 1);
		break;
	}
	case VALUE_INTERVAL:
	{
		buf_write(self, "\"", 1);
		buf_reserve(self, 512);
		int size = interval_write(&value->interval, (char*)self->position, 512);
		buf_advance(self, size);
		buf_write(self, "\"", 1);
		break;
	}
	case VALUE_TIMESTAMP:
	{
		buf_write(self, "\"", 1);
		buf_reserve(self, 128);
		int size = timestamp_write(value->integer, timezone, (char*)self->position, 128);
		buf_advance(self, size);
		buf_write(self, "\"", 1);
		break;
	}
	case VALUE_SET:
	case VALUE_MERGE:
		buf_write(self, "[", 1);
		value->obj->decode(value->obj, self, timezone);
		buf_write(self, "]", 1);
		break;
	// VALUE_GROUP
	default:
		error("operation unsupported");
		break;
	}

	// ]
	if (wrap)
		body_end(self);

	body_ensure_limit(self);
}

hot static inline void
body_empty(Buf* self)
{
	body_begin(self);
	body_end(self);
}

hot static inline void
body_add_comma(Buf* self)
{
	buf_write(self, ", ", 2);
}

hot static inline void
body_add_buf(Buf* self, Buf* buf, Timezone* tz)
{
	// wrap body in [] unless returning data is array or vector
	uint8_t* pos = buf->start;
	bool wrap = buf_empty(self) &&
	            (!data_is_array(pos) && !data_is_vector(pos));
	if (wrap)
		body_begin(self);
	json_export_pretty(self, tz, &pos);
	if (wrap)
		body_end(self);
	body_ensure_limit(self);
}

hot static inline void
body_add_raw(Buf* self, Buf* buf, Timezone* tz)
{
	uint8_t* pos = buf->start;
	json_export_pretty(self, tz, &pos);
	body_ensure_limit(self);
}

static inline void
body_error(Buf* self, Error* error)
{
	// {}
	auto msg = msg_error(error);
	guard_buf(msg);
	uint8_t* pos = msg_of(msg)->data;
	json_export(self, NULL, &pos);
}
