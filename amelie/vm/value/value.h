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
	VALUE_INT,
	VALUE_BOOL,
	VALUE_REAL,
	VALUE_NULL,
	VALUE_STRING,
	VALUE_OBJ,
	VALUE_ARRAY,
	VALUE_INTERVAL,
	VALUE_TIMESTAMP,
	VALUE_VECTOR,
	VALUE_AGG,
	VALUE_SET,
	VALUE_MERGE,
	VALUE_GROUP
} ValueType;

struct Value
{
	ValueType type;
	union
	{
		int64_t  integer;
		double   real;
		Str      string;
		Interval interval;
		Vector   vector;
		Agg      agg;
		struct {
			uint8_t* data;
			int      data_size;
		};
		Store*   store;
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

	if (unlikely(self->type == VALUE_SET   ||
	             self->type == VALUE_MERGE ||
	             self->type == VALUE_GROUP))
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
value_set_int(Value* self, int64_t data)
{
	self->type    = VALUE_INT;
	self->integer = data;
	self->buf     = NULL;
}

always_inline hot static inline void
value_set_bool(Value* self, bool data)
{
	self->type    = VALUE_BOOL;
	self->integer = data;
	self->buf     = NULL;
}

always_inline hot static inline void
value_set_real(Value* self, double data)
{
	self->type = VALUE_REAL;
	self->real = data;
	self->buf  = NULL;
}

always_inline hot static inline void
value_set_null(Value* self)
{
	self->type = VALUE_NULL;
	self->buf  = NULL;
}

always_inline hot static inline void
value_set_string(Value* self, Str* str, Buf* buf)
{
	self->type   = VALUE_STRING;
	self->string = *str;
	self->buf    = buf;
}

always_inline hot static inline void
value_set_obj(Value* self, uint8_t* data, int data_size, Buf* buf)
{
	self->type      = VALUE_OBJ;
	self->data      = data;
	self->data_size = data_size;
	self->buf       = buf;
}

always_inline hot static inline void
value_set_obj_buf(Value* self, Buf* buf)
{
	value_set_obj(self, buf->start, buf_size(buf), buf);
}

always_inline hot static inline void
value_set_array(Value* self, uint8_t* data, int data_size, Buf* buf)
{
	self->type      = VALUE_ARRAY;
	self->data      = data;
	self->data_size = data_size;
	self->buf       = buf;
}

always_inline hot static inline void
value_set_array_buf(Value* self, Buf* buf)
{
	value_set_array(self, buf->start, buf_size(buf), buf);
}

always_inline hot static inline void
value_set_interval(Value* self, Interval* iv)
{
	self->type     = VALUE_INTERVAL;
	self->interval = *iv;
}

always_inline hot static inline void
value_set_timestamp(Value* self, uint64_t value)
{
	self->type    = VALUE_TIMESTAMP;
	self->integer = value;
}

always_inline hot static inline void
value_set_vector(Value* self, Vector* vector, Buf* buf)
{
	self->type   = VALUE_VECTOR;
	self->vector = *vector;
	self->buf    = buf;
}

always_inline hot static inline void
value_set_agg(Value* self, Agg* agg)
{
	self->type = VALUE_AGG;
	self->agg  = *agg;
}

always_inline hot static inline void
value_set_set(Value* self, Store* store)
{
	self->type  = VALUE_SET;
	self->store = store;
	self->buf   = NULL;
}

always_inline hot static inline void
value_set_merge(Value* self, Store* store)
{
	self->type  = VALUE_MERGE;
	self->store = store;
	self->buf   = NULL;
}

always_inline hot static inline void
value_set_group(Value* self, Store* store)
{
	self->type  = VALUE_GROUP;
	self->store = store;
	self->buf   = NULL;
}

hot static inline void
value_read(Value* self, uint8_t* data, Buf* buf)
{
	switch (*data) {
	case AM_TRUE:
	case AM_FALSE:
	{
		bool boolean;
		data_read_bool(&data, &boolean);
		value_set_bool(self, boolean);
		break;
	}
	case AM_NULL:
	{
		value_set_null(self);
		break;
	}
	case AM_REAL32:
	case AM_REAL64:
	{
		double real;
		data_read_real(&data, &real);
		value_set_real(self, real);
		break;
	}
	case AM_INTV0 ... AM_INT64:
	{
		int64_t integer;
		data_read_integer(&data, &integer);
		value_set_int(self, integer);
		break;
	}
	case AM_STRINGV0 ... AM_STRING32:
	{
		Str string;
		data_read_string(&data, &string);
		value_set_string(self, &string, buf);
		break;
	}
	case AM_OBJ:
	{
		uint8_t* end = data;
		data_skip(&end);
		value_set_obj(self, data, end - data, buf);
		break;
	}
	case AM_ARRAY:
	{
		uint8_t* end = data;
		data_skip(&end);
		value_set_array(self, data, end - data, buf);
		break;
	}
	case AM_INTERVAL:
	{
		Interval iv;
		data_read_interval(&data, &iv);
		value_set_interval(self, &iv);
		break;
	}
	case AM_TIMESTAMP:
	{
		int64_t integer;
		data_read_timestamp(&data, &integer);
		value_set_timestamp(self, integer);
		break;
	}
	case AM_VECTOR:
	{
		Vector vector;
		data_read_vector(&data, &vector);
		value_set_vector(self, &vector, buf);
		break;
	}
	case AM_AGG:
	{
		Agg agg;
		data_read_agg(&data, &agg);
		value_set_agg(self, &agg);
		break;
	}
	default:
		error_data();
		break;
	}
}

hot static inline void
value_read_ref(Value* self, uint8_t* data, Buf* buf)
{
	value_read(self, data, buf);
	if (self->buf && buf)
		buf_ref(buf);
}

hot static inline void
value_read_arg(Value* self, Buf* args, int order)
{
	if (unlikely(! args))
		error("arguments are not defined");
	uint8_t* pos = args->start;
	if (! array_find(&pos, order))
		error("argument %d is not defined", order);
	value_read(self, pos, NULL);
}

always_inline hot static inline void
value_write(Value* self, Buf* buf)
{
	switch (self->type) {
	case VALUE_INT:
		encode_integer(buf, self->integer);
		break;
	case VALUE_BOOL:
		encode_bool(buf, self->integer);
		break;
	case VALUE_REAL:
		encode_real(buf, self->real);
		break;
	case VALUE_STRING:
		encode_string(buf, &self->string);
		break;
	case VALUE_NULL:
		encode_null(buf);
		break;
	case VALUE_OBJ:
	case VALUE_ARRAY:
		buf_write(buf, self->data, self->data_size);
		break;
	case VALUE_INTERVAL:
		encode_interval(buf, &self->interval);
		break;
	case VALUE_TIMESTAMP:
		encode_timestamp(buf, self->integer);
		break;
	case VALUE_VECTOR:
		encode_vector(buf, &self->vector);
		break;
	case VALUE_AGG:
		encode_agg(buf, &self->agg);
		break;
	case VALUE_SET:
	case VALUE_MERGE:
		store_encode(self->store, buf);
		break;
	// VALUE_GROUP
	default:
		assert(0);
		break;
	}
}

always_inline hot static inline void
value_write_data(Value* self, uint8_t** pos)
{
	switch (self->type) {
	case VALUE_INT:
		data_write_integer(pos, self->integer);
		break;
	case VALUE_BOOL:
		data_write_bool(pos, self->integer);
		break;
	case VALUE_REAL:
		data_write_real(pos, self->real);
		break;
	case VALUE_STRING:
		data_write_string(pos, &self->string);
		break;
	case VALUE_NULL:
		data_write_null(pos);
		break;
	case VALUE_OBJ:
	case VALUE_ARRAY:
		memcpy(*pos, self->data, self->data_size);
		*pos += self->data_size;
		break;
	case VALUE_INTERVAL:
		data_write_interval(pos, &self->interval);
		break;
	case VALUE_TIMESTAMP:
		data_write_timestamp(pos, self->integer);
		break;
	case VALUE_VECTOR:
		data_write_vector(pos, &self->vector);
		break;
	case VALUE_AGG:
		data_write_agg(pos, &self->agg);
		break;
	default:
		error("operation is not supported");
		break;
	}
}

always_inline hot static inline void
value_copy(Value* self, Value* src)
{
	switch (src->type) {
	case VALUE_INT:
		value_set_int(self, src->integer);
		break;
	case VALUE_BOOL:
		value_set_bool(self, src->integer);
		break;
	case VALUE_REAL:
		value_set_real(self, src->real);
		break;
	case VALUE_NULL:
		value_set_null(self);
		break;
	case VALUE_STRING:
		if (src->buf)
		{
			value_set_string(self, &src->string, src->buf);
			buf_ref(src->buf);
		} else
		{
			auto buf = buf_create();
			encode_string(buf, &src->string);
			uint8_t* pos = buf->start;
			Str string;
			data_read_string(&pos, &string);
			value_set_string(self, &string, buf);
		}
		break;
	case VALUE_OBJ:
		if (src->buf)
		{
			value_set_obj(self, src->data, src->data_size, src->buf);
			buf_ref(src->buf);
		} else
		{
			auto buf = buf_create();
			buf_write(buf, src->data, src->data_size);
			value_set_obj_buf(self, buf);
		}
		break;
	case VALUE_ARRAY:
		if (src->buf)
		{
			value_set_array(self, src->data, src->data_size, src->buf);
			buf_ref(src->buf);
		} else
		{
			auto buf = buf_create();
			buf_write(buf, src->data, src->data_size);
			value_set_array_buf(self, buf);
		}
		break;
	case VALUE_INTERVAL:
		value_set_interval(self, &src->interval);
		break;
	case VALUE_TIMESTAMP:
		value_set_timestamp(self, src->integer);
		break;
	case VALUE_VECTOR:
		if (src->buf)
		{
			value_set_vector(self, &src->vector, src->buf);
			buf_ref(src->buf);
		} else
		{
			auto buf = buf_create();
			encode_vector(buf, &src->vector);
			uint8_t* pos = buf->start;
			Vector vector;
			data_read_vector(&pos, &vector);
			value_set_vector(self, &vector, buf);
		}
		break;
	case VALUE_AGG:
		value_set_agg(self, &src->agg);
		break;
	case VALUE_SET:
	case VALUE_MERGE:
	{
		auto buf = buf_create();
		value_write(src, buf);
		value_read(self, buf->start, buf);
		break;
	}
	// VALUE_GROUP
	default:
		error("operation is not supported");
		break;
	}
}

always_inline hot static inline void
value_copy_ref(Value* self, Value* src)
{
	switch (src->type) {
	case VALUE_INT:
		value_set_int(self, src->integer);
		break;
	case VALUE_BOOL:
		value_set_bool(self, src->integer);
		break;
	case VALUE_REAL:
		value_set_real(self, src->real);
		break;
	case VALUE_NULL:
		value_set_null(self);
		break;
	case VALUE_STRING:
		value_set_string(self, &src->string, NULL);
		break;
	case VALUE_OBJ:
		value_set_obj(self, src->data, src->data_size, NULL);
		break;
	case VALUE_ARRAY:
		value_set_array(self, src->data, src->data_size, NULL);
		break;
	case VALUE_INTERVAL:
		value_set_interval(self, &src->interval);
		break;
	case VALUE_TIMESTAMP:
		value_set_timestamp(self, src->integer);
		break;
	case VALUE_VECTOR:
		value_set_vector(self, &src->vector, NULL);
		break;
	case VALUE_AGG:
		value_set_agg(self, &src->agg);
		break;
	case VALUE_SET:
	case VALUE_MERGE:
	{
		auto buf = buf_create();
		value_write(src, buf);
		value_read(self, buf->start, buf);
		break;
	}
	// VALUE_GROUP
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
	case VALUE_INT:
		name = "int";
		break;
	case VALUE_BOOL:
		name = "int";
		break;
	case VALUE_REAL:
		name = "real";
		break;
	case VALUE_NULL:
		name = "null";
		break;
	case VALUE_STRING:
		name = "string";
		break;
	case VALUE_OBJ:
		name = "object";
		break;
	case VALUE_ARRAY:
		name = "array";
		break;
	case VALUE_INTERVAL:
		name = "interval";
		break;
	case VALUE_TIMESTAMP:
		name = "timestamp";
		break;
	case VALUE_VECTOR:
		name = "vector";
		break;
	case VALUE_AGG:
		name = "aggregate";
		break;
	case VALUE_SET:
		name = "set";
		break;
	case VALUE_MERGE:
		name = "merge";
		break;
	case VALUE_GROUP:
		name = "group";
		break;
	}
	return name;
}
