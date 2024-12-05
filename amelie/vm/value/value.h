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

typedef struct Value Value;

struct Value
{
	Type type;
	union
	{
		int64_t   integer;
		double    dbl;
		Str       string;
		Interval  interval;
		Vector*   vector;
		Avg       avg;
		Store*    store;
		struct {
			uint8_t* json;
			int      json_size;
		};
	};
	Buf* buf;
};

always_inline hot static inline void
value_init(Value* self)
{
	memset(self, 0, sizeof(*self));
}

always_inline hot static inline void
value_free(Value* self)
{
	if (self->type == TYPE_NULL)
		return;

	if (unlikely(self->type == TYPE_SET ||
	             self->type == TYPE_MERGE))
		store_free(self->store);

	if (unlikely(self->buf))
	{
		buf_free(self->buf);
		self->buf = NULL;
	}

	self->type = TYPE_NULL;
}

always_inline hot static inline void
value_reset(Value* self)
{
	self->type = TYPE_NULL;
	self->buf  = NULL;
}

always_inline hot static inline void
value_move(Value* self, Value* from)
{
	value_free(self);
	*self = *from;
	value_reset(from);
}

always_inline hot static inline void
value_set_null(Value* self)
{
	self->type = TYPE_NULL;
}

always_inline hot static inline void
value_set_bool(Value* self, bool value)
{
	self->type    = TYPE_BOOL;
	self->integer = value;
}

always_inline hot static inline void
value_set_int(Value* self, int64_t value)
{
	self->type    = TYPE_INT;
	self->integer = value;
}

always_inline hot static inline void
value_set_double(Value* self, double value)
{
	self->type = TYPE_DOUBLE;
	self->dbl  = value;
}

always_inline hot static inline void
value_set_string(Value* self, Str* value, Buf* buf)
{
	self->type   = TYPE_STRING;
	self->string = *value;
	self->buf    = buf;
}

always_inline hot static inline void
value_set_json(Value* self, uint8_t* json, int json_size, Buf* buf)
{
	self->type      = TYPE_JSON;
	self->json      = json;
	self->json_size = json_size;
	self->buf       = buf;
}

always_inline hot static inline void
value_set_timestamp(Value* self, uint64_t value)
{
	self->type    = TYPE_TIMESTAMP;
	self->integer = value;
}

always_inline hot static inline void
value_set_interval(Value* self, Interval* value)
{
	self->type     = TYPE_INTERVAL;
	self->interval = *value;
}

always_inline hot static inline void
value_set_json_buf(Value* self, Buf* buf)
{
	value_set_json(self, buf->start, buf_size(buf), buf);
}

always_inline hot static inline void
value_set_vector(Value* self, Vector* value, Buf* buf)
{
	self->type   = TYPE_VECTOR;
	self->vector = value;
	self->buf    = buf;
}

always_inline hot static inline void
value_set_avg(Value* self, Avg* value)
{
	self->type = TYPE_AVG;
	self->avg  = *value;
}

always_inline hot static inline void
value_set_vector_buf(Value* self, Buf* buf)
{
	value_set_vector(self, (Vector*)buf->start, buf);
}

always_inline hot static inline void
value_set_set(Value* self, Store* store)
{
	self->type  = TYPE_SET;
	self->store = store;
}

always_inline hot static inline void
value_set_merge(Value* self, Store* store)
{
	self->type  = TYPE_MERGE;
	self->store = store;
}

always_inline hot static inline void
value_copy(Value* self, Value* src)
{
	switch (src->type) {
	case TYPE_NULL:
		value_set_null(self);
		break;
	case TYPE_BOOL:
		value_set_bool(self, src->integer);
		break;
	case TYPE_INT:
		value_set_int(self, src->integer);
		break;
	case TYPE_DOUBLE:
		value_set_double(self, src->dbl);
		break;
	case TYPE_STRING:
		value_set_string(self, &src->string, src->buf);
		if (src->buf)
			buf_ref(src->buf);
		break;
	case TYPE_JSON:
		value_set_json(self, src->json, src->json_size, src->buf);
		if (src->buf)
			buf_ref(src->buf);
		break;
	case TYPE_TIMESTAMP:
		value_set_timestamp(self, src->integer);
		break;
	case TYPE_INTERVAL:
		value_set_interval(self, &src->interval);
		break;
	case TYPE_VECTOR:
		value_set_vector(self, src->vector, src->buf);
		if (src->buf)
			buf_ref(src->buf);
		break;
	case TYPE_AVG:
		value_set_avg(self, &src->avg);
		break;
	case TYPE_SET:
		value_set_set(self, src->store);
		store_ref(src->store);
		break;
	case TYPE_MERGE:
		value_set_merge(self, src->store);
		store_ref(src->store);
		break;
	default:
		error("operation is not supported");
		break;
	}
}

static inline uint32_t
value_hash(Value* self, uint32_t hash)
{
	uint8_t* data;
	int      data_size;
	if (self->type == TYPE_STRING)
	{
		data = str_u8(&self->string);
		data_size = str_size(&self->string);
	} else
	{
		assert(self->type == TYPE_INT ||
		       self->type == TYPE_STRING ||
		       self->type == TYPE_TIMESTAMP);
		data = (uint8_t*)&self->integer;
		data_size = sizeof(self->integer);
	}
	return hash_murmur3_32(data, data_size, hash);
}
