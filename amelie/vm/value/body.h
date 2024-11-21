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
body_ensure_limit(Buf* self)
{
	auto limit = var_int_of(&config()->limit_send);
	if (unlikely((uint64_t)buf_size(self) >= limit))
		error("reply limit reached");
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
body_add(Buf* self, Value* value, Timezone* tz, bool pretty, bool wrap)
{
	// wrap body in [] unless returning array, vector, set or merge
	wrap = wrap && buf_empty(self);
	if (wrap)
	{
		if (value->type == TYPE_JSON && json_is_array(value->json))
			wrap = false;
		else
			wrap = (value->type != TYPE_VECTOR &&
			        value->type != TYPE_SET    &&
			        value->type != TYPE_MERGE);
	}
	if (wrap)
		body_begin(self);
	value_export(value, tz, pretty, self);
	if (wrap)
		body_end(self);
	body_ensure_limit(self);
}

hot static inline void
body_add_comma(Buf* self)
{
	buf_write(self, ", ", 2);
}

hot static inline void
body_add_buf(Buf* self, Buf* buf, Timezone* tz)
{
	// wrap body in [] unless returning data is array
	uint8_t* pos = buf->start;
	bool wrap = buf_empty(self) && !json_is_array(pos);
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
