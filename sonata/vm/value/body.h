#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

hot static inline void
body_add(Buf* self, Value* value)
{
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
		buf_printf(self, "%.*s", str_size(&value->string), str_of(&value->string));
		buf_write(self, "\"", 1);
		break;
	case VALUE_NULL:
		buf_write(self, "null", 4);
		break;
	case VALUE_DATA:
	{
		uint8_t* pos = value->data;
		json_export_pretty(self, &pos);
		break;
	}
	case VALUE_SET:
	case VALUE_MERGE:
		buf_write(self, "[", 1);
		value->obj->decode(value->obj, self);
		buf_write(self, "]", 1);
		break;
	// VALUE_GROUP
	default:
		assert(0);
		break;
	}
}

hot static inline void
body_add_buf(Buf* self, Buf* buf)
{
	uint8_t* pos = buf->start;
	json_export_pretty(self, &pos);
}

hot static inline void
body_add_comma(Buf* self)
{
	buf_write(self, ", ", 2);
}

static inline void
body_error(Buf* self, Error* error)
{
	// {}
	auto msg = msg_error(error);
	guard_buf(msg);
	uint8_t* pos = msg_of(msg)->data;
	json_export(self, &pos);
}
