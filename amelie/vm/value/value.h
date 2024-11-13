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

typedef enum
{
	VALUE_NONE,
	VALUE_NULL,
	VALUE_BOOL,
	VALUE_INT,
	VALUE_DOUBLE,
	VALUE_TIMESTAMP,
	VALUE_INTERVAL,
	VALUE_STRING,
	VALUE_JSON,
	VALUE_VECTOR,
	VALUE_AVG,
	VALUE_SET,
	VALUE_MERGE,
	VALUE_MAX
} ValueType;

struct Value
{
	ValueType type;
	union
	{
		int64_t   integer;
		double    dbl;
		Str       string;
		Interval  interval;
		Vector*   vector;
		Avg       avg;
		Store*    store;
		union {
			uint8_t* data;
			int      data_size;
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
	if (self->type == VALUE_NONE)
		return;

	if (unlikely(self->buf))
	{
		buf_free(self->buf);
		self->buf = NULL;
	}

	if (unlikely(self->type == VALUE_SET ||
	             self->type == VALUE_MERGE))
		store_free(self->store);

	self->type = VALUE_NONE;
}

always_inline hot static inline void
value_reset(Value* self)
{
	self->type = VALUE_NONE;
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
	self->type = VALUE_NULL;
}

always_inline hot static inline void
value_set_bool(Value* self, bool value)
{
	self->type    = VALUE_BOOL;
	self->integer = value;
}

always_inline hot static inline void
value_set_int(Value* self, int64_t value)
{
	self->type    = VALUE_INT;
	self->integer = value;
}

always_inline hot static inline void
value_set_double(Value* self, double value)
{
	self->type = VALUE_DOUBLE;
	self->dbl  = value;
}

always_inline hot static inline void
value_set_timestamp(Value* self, uint64_t value)
{
	self->type    = VALUE_TIMESTAMP;
	self->integer = value;
}

always_inline hot static inline void
value_set_interval(Value* self, Interval* value)
{
	self->type     = VALUE_INTERVAL;
	self->interval = *value;
}

always_inline hot static inline void
value_set_string(Value* self, Str* value, Buf* buf)
{
	self->type   = VALUE_STRING;
	self->string = *value;
	self->buf    = buf;
}

always_inline hot static inline void
value_set_json(Value* self, uint8_t* data, int data_size, Buf* buf)
{
	self->type      = VALUE_JSON;
	self->data      = data;
	self->data_size = data_size;
	self->buf       = buf;
}

always_inline hot static inline void
value_set_json_buf(Value* self, Buf* buf)
{
	value_set_json(self, buf->start, buf_size(buf), buf);
}

always_inline hot static inline void
value_set_vector(Value* self, Vector* value, Buf* buf)
{
	self->type   = VALUE_VECTOR;
	self->vector = value;
	self->buf    = buf;
}

always_inline hot static inline void
value_set_avg(Value* self, Avg* value)
{
	self->type = VALUE_AVG;
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
	self->type  = VALUE_SET;
	self->store = store;
}

always_inline hot static inline void
value_set_merge(Value* self, Store* store)
{
	self->type  = VALUE_MERGE;
	self->store = store;
}

always_inline hot static inline void
value_copy(Value* self, Value* src)
{
	switch (src->type) {
	case VALUE_NULL:
		value_set_null(self);
		break;
	case VALUE_BOOL:
		value_set_bool(self, src->integer);
		break;
	case VALUE_INT:
		value_set_int(self, src->integer);
		break;
	case VALUE_DOUBLE:
		value_set_double(self, src->dbl);
		break;
	case VALUE_TIMESTAMP:
		value_set_timestamp(self, src->integer);
		break;
	case VALUE_INTERVAL:
		value_set_interval(self, &src->interval);
		break;
	case VALUE_STRING:
		value_set_string(self, &src->string, src->buf);
		if (src->buf)
			buf_ref(src->buf);
		break;
	case VALUE_JSON:
		value_set_json(self, src->data, src->data_size, src->buf);
		if (src->buf)
			buf_ref(src->buf);
		break;
	case VALUE_VECTOR:
		value_set_vector(self, src->vector, src->buf);
		if (src->buf)
			buf_ref(src->buf);
		break;
	case VALUE_AVG:
		value_set_avg(self, &src->avg);
		break;
	case VALUE_SET:
	case VALUE_MERGE:
	default:
		error("operation is not supported");
		break;
	}
}

static inline char*
value_type_to_string(ValueType type)
{
	char* name;
	switch (type) {
	case VALUE_NONE:
		name = "none";
		break;
	case VALUE_NULL:
		name = "null";
		break;
	case VALUE_BOOL:
		name = "int";
		break;
	case VALUE_INT:
		name = "int";
		break;
	case VALUE_DOUBLE:
		name = "double";
		break;
	case VALUE_TIMESTAMP:
		name = "timestamp";
		break;
	case VALUE_INTERVAL:
		name = "interval";
		break;
	case VALUE_STRING:
		name = "string";
		break;
	case VALUE_JSON:
		name = "json";
		break;
	case VALUE_VECTOR:
		name = "vector";
		break;
	case VALUE_AVG:
		name = "avg";
		break;
	case VALUE_SET:
		name = "set";
		break;
	case VALUE_MERGE:
		name = "merge";
		break;
	case VALUE_MAX:
		abort();
		break;
	}
	return name;
}
