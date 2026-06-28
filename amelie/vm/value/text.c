
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
#include <amelie_value.h>

static inline int
estimate_int(int64_t value)
{
	if (value == 0)
		return 1;
	if (value == INT64_MIN)
		return 20;

	// -
	auto len = 0;
	if (value < 0)
	{
		value = -value;
		len   = 1;
	}
	for (; value > 0; len++)
		value /= 10;

	return len;
}

hot int
value_print_estimate(Value* self, Timezone* tz, bool pretty, Buf* buf)
{
	switch (self->type) {
	case TYPE_NULL:
		return 4;
	case TYPE_BOOL:
		return 5;
	case TYPE_INT:
		return estimate_int(self->integer);
	case TYPE_DOUBLE:
	{
		buf_reset(buf);
		buf_format(buf, "{g}", self->dbl);
		return buf_size(buf);
	}
	case TYPE_DATE:
		return 10;
	case TYPE_TIMESTAMP:
		return 32;
	case TYPE_INTERVAL:
	{
		buf_reset(buf);
		buf_reserve(buf, 128);
		auto size = interval_get(&self->interval, (char*)buf->position, 128);
		return size;
	}
	case TYPE_UUID:
		return UUID_SZ - 1;
	case TYPE_STRING:
		return utf8_strlen(&self->string);
	case TYPE_JSON:
	{
		buf_reset(buf);
		auto pos = self->json;
		json_export_as(buf, tz, pretty, 0, &pos);
		Str str;
		buf_str(buf, &str);
		return utf8_strlen(&str);
	}
	case TYPE_VECTOR:
	{
		buf_reset(buf);
		buf_write(buf, "[", 1);
		for (auto i = 0; i < self->vector_dim; i++)
			buf_format(buf, "{g}{s}", self->vector[i],
			           i != self->vector_dim - 1 ? ", ": "");
		buf_write(buf, "]", 1);
		return buf_size(buf);
	}
	// TYPE_STORE
	// TYPE_CURSOR
	// TYPE_CURSOR_STORE
	// TYPE_CURSOR_JSON
	default:
		error("operation unsupported");
		break;
	}
	return -1;
}
