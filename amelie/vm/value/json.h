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
value_encode(Value* self, Timezone* tz, Buf* buf)
{
	switch (self->type) {
	case TYPE_NULL:
		encode_null(buf);
		break;
	case TYPE_BOOL:
		encode_bool(buf, self->integer);
		break;
	case TYPE_INT:
		encode_int(buf, self->integer);
		break;
	case TYPE_DOUBLE:
		encode_real(buf, self->dbl);
		break;
	case TYPE_STRING:
		encode_str(buf, &self->string);
		break;
	case TYPE_JSON:
		buf_write(buf, self->json, data_sizeof(self->json));
		break;
	case TYPE_DATE:
		encode_date(buf, self->integer);
		break;
	case TYPE_TIMESTAMP:
		encode_timestamp(buf, tz, self->integer);
		break;
	case TYPE_INTERVAL:
		encode_interval(buf, &self->interval);
		break;
	case TYPE_VECTOR:
		encode_vector(buf, self->vector);
		break;
	case TYPE_UUID:
		encode_uuid(buf, &self->uuid);
		break;
	// TYPE_STORE
	// TYPE_CURSOR
	// TYPE_CURSOR_STORE
	// TYPE_CURSOR_JSON
	default:
		error("operation unsupported");
		break;
	}
}

hot static inline void
value_decode(Value* self, uint8_t* json, Buf* buf)
{
	switch (*json) {
	case DATA_TRUE:
	case DATA_FALSE:
	{
		bool boolean;
		unpack_bool(&json, &boolean);
		value_set_bool(self, boolean);
		break;
	}
	case DATA_NULL:
	{
		value_set_null(self);
		break;
	}
	case DATA_REAL32:
	case DATA_REAL64:
	{
		double real;
		unpack_real(&json, &real);
		value_set_double(self, real);
		break;
	}
	case DATA_INTV0 ... DATA_INT64:
	{
		int64_t integer;
		unpack_int(&json, &integer);
		value_set_int(self, integer);
		break;
	}
	case DATA_STRV0 ... DATA_STR32:
	{
		Str string;
		unpack_str(&json, &string);
		value_set_string(self, &string, buf);
		break;
	}
	case DATA_OBJ:
	case DATA_ARRAY:
		value_set_json(self, json, data_sizeof(json), buf);
		break;
	default:
		data_error_read();
		break;
	}
}

hot static inline void
value_decode_ref(Value* self, uint8_t* json, Buf* buf)
{
	value_decode(self, json, buf);
	if (self->buf && buf)
		buf_ref(buf);
}

hot static inline void
value_export_as(Value* self, Timezone* tz, bool pretty, int deep, Buf* buf)
{
	switch (self->type) {
	case TYPE_NULL:
		buf_write(buf, "null", 4);
		break;
	case TYPE_BOOL:
		if (self->integer > 0)
			buf_write(buf, "true", 4);
		else
			buf_write(buf, "false", 5);
		break;
	case TYPE_INT:
		buf_printf(buf, "%" PRIi64, self->integer);
		break;
	case TYPE_DOUBLE:
		buf_printf(buf, "%g", self->dbl);
		break;
	case TYPE_STRING:
		buf_write(buf, "\"", 1);
		escape_str(buf, &self->string);
		buf_write(buf, "\"", 1);
		break;
	case TYPE_JSON:
	{
		uint8_t* pos = self->json;
		json_export_as(buf, tz, pretty, deep, &pos);
		break;
	}
	case TYPE_DATE:
	{
		buf_write(buf, "\"", 1);
		buf_reserve(buf, 128);
		int size = date_get(self->integer, (char*)buf->position, 128);
		buf_advance(buf, size);
		buf_write(buf, "\"", 1);
		break;
	}
	case TYPE_TIMESTAMP:
	{
		buf_write(buf, "\"", 1);
		buf_reserve(buf, 128);
		int size = timestamp_get(self->integer, tz, (char*)buf->position, 128);
		buf_advance(buf, size);
		buf_write(buf, "\"", 1);
		break;
	}
	case TYPE_INTERVAL:
	{
		buf_write(buf, "\"", 1);
		buf_reserve(buf, 512);
		int size = interval_get(&self->interval, (char*)buf->position, 512);
		buf_advance(buf, size);
		buf_write(buf, "\"", 1);
		break;
	}
	case TYPE_VECTOR:
	{
		buf_write(buf, "[", 1);
		for (uint32_t i = 0; i < self->vector->size; i++)
			buf_printf(buf, "%g%s", self->vector->value[i],
			           i != self->vector->size - 1 ? ", ": "");
		buf_write(buf, "]", 1);
		break;
	}
	case TYPE_UUID:
	{
		buf_write(buf, "\"", 1);
		buf_reserve(buf, UUID_SZ);
		uuid_get(&self->uuid, (char*)buf->position, UUID_SZ);
		buf_advance(buf, UUID_SZ - 1);
		buf_write(buf, "\"", 1);
		break;
	}
	// TYPE_STORE
	// TYPE_CURSOR
	// TYPE_CURSOR_STORE
	// TYPE_CURSOR_JSON
	default:
		error("operation unsupported");
		break;
	}
}

hot static inline void
value_export(Value* self, Timezone* tz, bool pretty, Buf* buf)
{
	value_export_as(self, tz, pretty, 0, buf);
}
