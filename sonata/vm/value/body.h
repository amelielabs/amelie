#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Body Body;

struct Body
{
	Buf buf;
};

static inline void
body_init(Body* self)
{
	buf_init(&self->buf);
}

static inline void
body_free(Body* self)
{
	buf_free(&self->buf);
}

static inline void
body_reset(Body* self)
{
	buf_reset(&self->buf);
}

hot static inline void
body_add(Body* self, Value* value)
{
	auto buf = &self->buf;
	switch (value->type) {
	case VALUE_INT:
		buf_printf(buf, "%" PRIi64, value->integer);
		break;
	case VALUE_BOOL:
		if (value->integer > 0)
			buf_write(buf, "true", 4);
		else
			buf_write(buf, "false", 5);
		break;
	case VALUE_REAL:
		buf_printf(buf, "%g", value->real);
		break;
	case VALUE_STRING:
		buf_write(buf, "\"", 1);
		buf_printf(buf, "%.*s", str_size(&value->string), str_of(&value->string));
		buf_write(buf, "\"", 1);
		break;
	case VALUE_NULL:
		buf_write(buf, "null", 4);
		break;
	case VALUE_DATA:
	{
		uint8_t* pos = value->data;
		json_export_pretty(buf, &pos); 
		break;
	}
	case VALUE_SET:
	case VALUE_MERGE:
		buf_write(&self->buf, "[", 1);
		value->obj->decode(value->obj, self);
		buf_write(&self->buf, "]", 1);
		break;
	// VALUE_GROUP
	default:
		assert(0);
		break;
	}
}

hot static inline void
body_add_buf(Body* self, Buf* buf)
{
	uint8_t* pos = buf->start;
	json_export_pretty(&self->buf, &pos); 
}

hot static inline void
body_add_comma(Body* self)
{
	buf_write(&self->buf, ", ", 2);
}

static inline void
body_error(Body* self, Error* error)
{
	// {}
	auto msg = msg_error(error);
	guard_buf(msg);
	uint8_t* pos = msg_of(msg)->data;
	json_export(&self->buf, &pos); 
}
