
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>

void
json_export_as(Buf* data, Timezone* timezone, bool pretty, int deep, uint8_t** pos)
{
	char buf[256];
	int  buf_len;
	switch (**pos) {
	case JSON_NULL:
		json_read_null(pos);
		buf_write(data, "null", 4);
		break;
	case JSON_TRUE:
	case JSON_FALSE:
	{
		bool value;
		json_read_bool(pos, &value);
		if (value)
			buf_write(data, "true", 4);
		else
			buf_write(data, "false", 5);
		break;
	}
	case JSON_REAL32:
	case JSON_REAL64:
	{
		double value;
		json_read_real(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%g", value);
		buf_write(data, buf, buf_len);
		break;
	}
	case JSON_INTV0 ... JSON_INT64:
	{
		int64_t value;
		json_read_integer(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%" PRIi64, value);
		buf_write(data, buf, buf_len);
		break;
	}
	case JSON_STRINGV0 ... JSON_STRING32:
	{
		Str str;
		json_read_string(pos, &str);
		buf_write(data, "\"", 1);
		escape_string_raw(data, &str);
		buf_write(data, "\"", 1);
		break;
	}
	case JSON_ARRAY:
	{
		json_read_array(pos);
		buf_write(data, "[", 1);
		while (! json_read_array_end(pos))
		{
			json_export_as(data, timezone, pretty, deep, pos);
			// ,
			if (! json_is_array_end(*pos))
				buf_write(data, ", ", 2);
		}
		buf_write(data, "]", 1);
		break;
	}
	case JSON_OBJ:
	{
		json_read_obj(pos);
		if (pretty)
		{
			// {}
			if (json_read_obj_end(pos))
			{
				buf_write(data, "{}", 2);
				break;
			}
			buf_write(data, "{\n", 2);
			while (! json_read_obj_end(pos))
			{
				for (int i = 0; i < deep + 1; i++)
					buf_write(data, "  ", 2);
				// key
				json_export_as(data, timezone, pretty, deep + 1, pos);
				buf_write(data, ": ", 2);
				// value
				json_export_as(data, timezone, pretty, deep + 1, pos);
				// ,
				if (json_is_obj_end(*pos))
					buf_write(data, "\n", 1);
				else
					buf_write(data, ",\n", 2);
			}
			for (int i = 0; i < deep; i++)
				buf_write(data, "  ", 2);
			buf_write(data, "}", 1);
		} else
		{
			buf_write(data, "{", 1);
			while (! json_read_obj_end(pos))
			{
				// key
				json_export_as(data, timezone, pretty, deep + 1, pos);
				buf_write(data, ": ", 2);
				// value
				json_export_as(data, timezone, pretty, deep + 1, pos);
				// ,
				if (! json_is_obj_end(*pos))
					buf_write(data, ", ", 2);
			}
			buf_write(data, "}", 1);
		}
		break;
	}
	default:
		json_error_read();
		break;
	}
}

void
json_export(Buf* self, Timezone* timezone, uint8_t** pos)
{
	json_export_as(self, timezone, false, 0, pos);
}

void
json_export_pretty(Buf* self, Timezone* timezone, uint8_t** pos)
{
	json_export_as(self, timezone, true, 0, pos);
}
