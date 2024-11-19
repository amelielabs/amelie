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
	case VALUE_NULL:
		encode_null(buf);
		break;
	case VALUE_BOOL:
		encode_bool(buf, self->integer);
		break;
	case VALUE_INT:
		encode_integer(buf, self->integer);
		break;
	case VALUE_DOUBLE:
		encode_real(buf, self->dbl);
		break;
	case VALUE_STRING:
		encode_string(buf, &self->string);
		break;
	case VALUE_JSON:
		buf_write(buf, self->data, data_sizeof(self->data));
		break;
	case VALUE_TIMESTAMP:
	{
		auto offset = buf_size(buf);
		encode_string32(buf, 0);
		buf_reserve(buf, 128);
		int size = timestamp_write(self->integer, tz, (char*)buf->position, 128);
		buf_advance(buf, size);
		uint8_t* pos = buf->start + offset;
		data_write_string32(&pos, size);
		break;
	}
	case VALUE_INTERVAL:
	{
		auto offset = buf_size(buf);
		encode_string32(buf, 0);
		buf_reserve(buf, 256);
		int size = interval_write(&self->interval, (char*)buf->position, 256);
		buf_advance(buf, size);
		uint8_t* pos = buf->start + offset;
		data_write_string32(&pos, size);
		break;
	}
	case VALUE_VECTOR:
	{
		encode_array(buf);
		for (uint32_t i = 0; i < self->vector->size; i++)
			encode_real(buf, self->vector->value[i]);
		encode_array_end(buf);
		break;
	}
	case VALUE_SET:
	case VALUE_MERGE:
		store_encode(self->store, tz, buf);
		break;
	default:
		assert(0);
		break;
	}
}

hot static inline void
value_decode(Value* self, uint8_t* data, Buf* buf)
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
		value_set_double(self, real);
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
	case AM_ARRAY:
		value_set_json(self, data, data_sizeof(data), buf);
		break;
	default:
		error_data();
		break;
	}
}

hot static inline void
value_decode_ref(Value* self, uint8_t* data, Buf* buf)
{
	value_decode(self, data, buf);
	if (self->buf && buf)
		buf_ref(buf);
}

hot static inline void
value_export(Value* self, Timezone* tz, bool pretty, Buf* buf)
{
	switch (self->type) {
	case VALUE_NULL:
		buf_write(buf, "null", 4);
		break;
	case VALUE_BOOL:
		if (self->integer > 0)
			buf_write(buf, "true", 4);
		else
			buf_write(buf, "false", 5);
		break;
	case VALUE_INT:
		buf_printf(buf, "%" PRIi64, self->integer);
		break;
	case VALUE_DOUBLE:
		buf_printf(buf, "%g", self->dbl);
		break;
	case VALUE_STRING:
		buf_write(buf, "\"", 1);
		escape_string_raw(buf, &self->string);
		buf_write(buf, "\"", 1);
		break;
	case VALUE_JSON:
	{
		uint8_t* pos = self->data;
		if (pretty)
			json_export_pretty(buf, tz, &pos);
		else
			json_export(buf, tz, &pos);
		break;
	}
	case VALUE_TIMESTAMP:
	{
		buf_write(buf, "\"", 1);
		buf_reserve(buf, 128);
		int size = timestamp_write(self->integer, tz, (char*)buf->position, 128);
		buf_advance(buf, size);
		buf_write(buf, "\"", 1);
		break;
	}
	case VALUE_INTERVAL:
	{
		buf_write(buf, "\"", 1);
		buf_reserve(buf, 512);
		int size = interval_write(&self->interval, (char*)buf->position, 512);
		buf_advance(buf, size);
		buf_write(buf, "\"", 1);
		break;
	}
	case VALUE_VECTOR:
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
	case VALUE_SET:
	case VALUE_MERGE:
		buf_write(buf, "[", 1);
		store_export(self->store, tz, buf);
		buf_write(buf, "]", 1);
		break;
	default:
		error("operation unsupported");
		break;
	}
}
