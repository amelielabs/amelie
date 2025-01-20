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
		encode_integer(buf, self->integer);
		break;
	case TYPE_DOUBLE:
		encode_real(buf, self->dbl);
		break;
	case TYPE_STRING:
		encode_string(buf, &self->string);
		break;
	case TYPE_JSON:
		buf_write(buf, self->json, json_sizeof(self->json));
		break;
	case TYPE_DATE:
	{
		auto offset = buf_size(buf);
		encode_string32(buf, 0);
		buf_reserve(buf, 32);
		int size = date_get(self->integer, (char*)buf->position, 128);
		buf_advance(buf, size);
		uint8_t* pos = buf->start + offset;
		json_write_string32(&pos, size);
		break;
	}
	case TYPE_TIMESTAMP:
	{
		auto offset = buf_size(buf);
		encode_string32(buf, 0);
		buf_reserve(buf, 128);
		int size = timestamp_get(self->integer, tz, (char*)buf->position, 128);
		buf_advance(buf, size);
		uint8_t* pos = buf->start + offset;
		json_write_string32(&pos, size);
		break;
	}
	case TYPE_INTERVAL:
	{
		auto offset = buf_size(buf);
		encode_string32(buf, 0);
		buf_reserve(buf, 256);
		int size = interval_get(&self->interval, (char*)buf->position, 256);
		buf_advance(buf, size);
		uint8_t* pos = buf->start + offset;
		json_write_string32(&pos, size);
		break;
	}
	case TYPE_VECTOR:
	{
		encode_array(buf);
		for (uint32_t i = 0; i < self->vector->size; i++)
			encode_real(buf, self->vector->value[i]);
		encode_array_end(buf);
		break;
	}
	case TYPE_UUID:
	{
		encode_uuid(buf, &self->uuid);
		break;
	}
	// TYPE_STORE
	default:
		error("operation unsupported");
		break;
	}
}

hot static inline void
value_decode(Value* self, uint8_t* json, Buf* buf)
{
	switch (*json) {
	case JSON_TRUE:
	case JSON_FALSE:
	{
		bool boolean;
		json_read_bool(&json, &boolean);
		value_set_bool(self, boolean);
		break;
	}
	case JSON_NULL:
	{
		value_set_null(self);
		break;
	}
	case JSON_REAL32:
	case JSON_REAL64:
	{
		double real;
		json_read_real(&json, &real);
		value_set_double(self, real);
		break;
	}
	case JSON_INTV0 ... JSON_INT64:
	{
		int64_t integer;
		json_read_integer(&json, &integer);
		value_set_int(self, integer);
		break;
	}
	case JSON_STRINGV0 ... JSON_STRING32:
	{
		Str string;
		json_read_string(&json, &string);
		value_set_string(self, &string, buf);
		break;
	}
	case JSON_OBJ:
	case JSON_ARRAY:
		value_set_json(self, json, json_sizeof(json), buf);
		break;
	default:
		json_error_read();
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
value_export(Value* self, Timezone* tz, bool pretty, Buf* buf)
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
		escape_string_raw(buf, &self->string);
		buf_write(buf, "\"", 1);
		break;
	case TYPE_JSON:
	{
		uint8_t* pos = self->json;
		if (pretty)
			json_export_pretty(buf, tz, &pos);
		else
			json_export(buf, tz, &pos);
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
		{
			char val[32];
			auto val_len = snprintf(val, sizeof(val), "%g%s", self->vector->value[i],
			                        i != self->vector->size - 1 ? ", ": "");
			buf_write(buf, val, val_len);
		}
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
	default:
		error("operation unsupported");
		break;
	}
}
