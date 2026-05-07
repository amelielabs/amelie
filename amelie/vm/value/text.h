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
value_print(Value* self, Timezone* tz, bool pretty, Buf* buf)
{
	switch (self->type) {
	case TYPE_NULL:
		break;
	case TYPE_BOOL:
		if (self->integer > 0)
			buf_write(buf, "true", 4);
		else
			buf_write(buf, "false", 5);
		break;
	case TYPE_INT:
		buf_format(buf, "{i64}", self->integer);
		break;
	case TYPE_DOUBLE:
		buf_format(buf, "{g}", self->dbl);
		break;
	case TYPE_STRING:
	{
		if (pretty)
			unescape_str(buf, &self->string);
		else
			escape_str(buf, &self->string);
		break;
	}
	case TYPE_JSON:
	{
		uint8_t* pos = self->json;
		json_export_as(buf, tz, pretty, 0, &pos);
		break;
	}
	case TYPE_DATE:
	{
		buf_reserve(buf, 16);
		int size = date_get(self->integer, (char*)buf->position, 16);
		buf_advance(buf, size);
		break;
	}
	case TYPE_TIMESTAMP:
	{
		buf_reserve(buf, 32);
		int size = timestamp_get(self->integer, tz, (char*)buf->position, 32);
		buf_advance(buf, size);
		break;
	}
	case TYPE_INTERVAL:
	{
		buf_reserve(buf, 128);
		int size = interval_get(&self->interval, (char*)buf->position, 128);
		buf_advance(buf, size);
		break;
	}
	case TYPE_VECTOR:
	{
		buf_write(buf, "[", 1);
		for (uint32_t i = 0; i < self->vector->size; i++)
			buf_format(buf, "{g}{s}", self->vector->value[i],
			           i != self->vector->size - 1 ? ", ": "");
		buf_write(buf, "]", 1);
		break;
	}
	case TYPE_UUID:
	{
		buf_reserve(buf, UUID_SZ);
		uuid_get(&self->uuid, (char*)buf->position, UUID_SZ);
		buf_advance(buf, UUID_SZ - 1);
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

int value_print_estimate(Value*, Timezone*, bool, Buf*);
