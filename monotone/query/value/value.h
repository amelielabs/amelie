#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ValueObj ValueObj;
typedef struct Value    Value;

enum
{
	VALUE_NONE,
	VALUE_INT,
	VALUE_BOOL,
	VALUE_REAL,
	VALUE_NULL,
	VALUE_STRING,
	VALUE_DATA,
	VALUE_SET,
	VALUE_MERGE,
	VALUE_GROUP
};

struct ValueObj
{
	void (*free)(ValueObj*);
	void (*convert)(ValueObj*, Buf*);
};

struct Value
{
	int type;
	union
	{
		int64_t integer;
		double  real;
		Str     string;
		struct {
			uint8_t* data;
			int      data_size;
		};
		ValueObj* obj;
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
		buf_free(self->buf);

	if (unlikely(self->type == VALUE_SET   ||
	             self->type == VALUE_MERGE ||
	             self->type == VALUE_GROUP))
		self->obj->free(self->obj);

	self->type = VALUE_NONE;
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
value_set_data(Value* self, uint8_t* data, int data_size, Buf* buf)
{
	self->type      = VALUE_DATA;
	self->data      = data;
	self->data_size = data_size;
	self->buf       = buf;
}

always_inline hot static inline void
value_set_data_from(Value* self, Buf* buf)
{
	auto msg = msg_of(buf);
	value_set_data(self, msg->data, msg_data_size(msg), buf);
}

always_inline hot static inline void
value_set_set(Value* self, ValueObj* obj)
{
	self->type = VALUE_SET;
	self->obj  = obj;
	self->buf  = NULL;
}

always_inline hot static inline void
value_set_merge(Value* self, ValueObj* obj)
{
	self->type = VALUE_MERGE;
	self->obj  = obj;
	self->buf  = NULL;
}

always_inline hot static inline void
value_set_group(Value* self, ValueObj* obj)
{
	self->type = VALUE_GROUP;
	self->obj  = obj;
	self->buf  = NULL;
}

hot static inline void
value_read(Value* self, uint8_t* data, Buf* buf)
{
	switch (*data) {
	case MN_TRUE:
	case MN_FALSE:
	{
		bool boolean;
		data_read_bool(&data, &boolean);
		value_set_bool(self, boolean);
		break;
	}
	case MN_NULL:
	{
		value_set_null(self);
		break;
	}
	case MN_REAL32:
	case MN_REAL64:
	{
		double real;
		data_read_real(&data, &real);
		value_set_real(self, real);
		break;
	}
	case MN_INTV0 ... MN_INT64:
	{
		int64_t integer;
		data_read_integer(&data, &integer);
		value_set_int(self, integer);
		break;
	}
	case MN_ARRAYV0 ... MN_ARRAY32:
	case MN_MAPV0 ... MN_MAP32:
	{
		uint8_t* end = data;
		data_skip(&end);
		value_set_data(self, data, end - data, buf);
		if (buf)
			buf_ref(buf);
		break;
	}
	case MN_STRINGV0 ... MN_STRING32:
	{
		Str string;
		data_read_string(&data, &string);
		value_set_string(self, &string, buf);
		if (buf)
			buf_ref(buf);
		break;
	}
	default:
		error_data();
		break;
	}
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
	case VALUE_DATA:
		buf_write(buf, self->data, self->data_size);
		break;
	case VALUE_SET:
		self->obj->convert(self->obj, buf);
		break;
	// VALUE_MERGE
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
	case VALUE_DATA:
		memcpy(*pos, self->data, self->data_size);
		*pos += self->data_size;
		break;
	default:
		error("operation is not supported");
		break;
	}
}

always_inline hot static inline int
value_size(Value* self)
{
	switch (self->type) {
	case VALUE_INT:
		return data_size_integer(self->integer);
	case VALUE_BOOL:
		return data_size_bool();
	case VALUE_REAL:
		return data_size_real(self->real);
	case VALUE_STRING:
		return data_size_string(str_size(&self->string));
	case VALUE_NULL:
		return data_size_null();
	case VALUE_DATA:
		return self->data_size;
	default:
		error("operation is not supported");
		break;
	}
	return 0;
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
			auto buf = msg_create(MSG_OBJECT);
			encode_string(buf, &src->string);
			msg_end(buf);

			uint8_t* pos = msg_of(buf)->data;
			Str string;
			data_read_string(&pos, &string);
			value_set_string(self, &string, buf);
		}
		break;
	case VALUE_DATA:
		if (src->buf)
		{
			value_set_data(self, src->data, src->data_size, src->buf);
			buf_ref(src->buf);
		} else
		{
			auto buf = msg_create(MSG_OBJECT);
			buf_write(buf, src->data, src->data_size);
			msg_end(buf);
			auto msg = msg_of(buf);
			value_set_data(self, msg->data, msg_data_size(msg), buf);
		}
		break;

	case VALUE_SET:
	{
		auto buf = msg_create(MSG_OBJECT);
		value_write(src, buf);
		msg_end(buf);
		auto msg = msg_of(buf);
		value_set_data(self, msg->data, msg_data_size(msg), buf);
		break;
	}
	// VALUE_MERGE
	// VALUE_GROUP
	default:
		error("operation is not supported");
		break;
	}
}
