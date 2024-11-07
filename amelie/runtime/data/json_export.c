
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
#include <amelie_data.h>

static void
json_export_as(Buf* data, Timezone* timezone, bool pretty, int deep, uint8_t** pos)
{
	char buf[256];
	int  buf_len;
	switch (**pos) {
	case AM_NULL:
		data_read_null(pos);
		buf_write(data, "null", 4);
		break;
	case AM_TRUE:
	case AM_FALSE:
	{
		bool value;
		data_read_bool(pos, &value);
		if (value)
			buf_write(data, "true", 4);
		else
			buf_write(data, "false", 5);
		break;
	}
	case AM_REAL32:
	case AM_REAL64:
	{
		double value;
		data_read_real(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%g", value);
		buf_write(data, buf, buf_len);
		break;
	}
	case AM_INTV0 ... AM_INT64:
	{
		int64_t value;
		data_read_integer(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%" PRIi64, value);
		buf_write(data, buf, buf_len);
		break;
	}
	case AM_STRINGV0 ... AM_STRING32:
	{
		Str str;
		data_read_string(pos, &str);
		buf_write(data, "\"", 1);
		escape_string_raw(data, &str);
		buf_write(data, "\"", 1);
		break;
	}
	case AM_ARRAY:
	{
		data_read_array(pos);
		buf_write(data, "[", 1);
		while (! data_read_array_end(pos))
		{
			json_export_as(data, timezone, pretty, deep, pos);
			// ,
			if (! data_is_array_end(*pos))
				buf_write(data, ", ", 2);
		}
		buf_write(data, "]", 1);
		break;
	}
	case AM_OBJ:
	{
		data_read_obj(pos);
		if (pretty)
		{
			// {}
			if (data_read_obj_end(pos))
			{
				buf_write(data, "{}", 2);
				break;
			}
			buf_write(data, "{\n", 2);
			while (! data_read_obj_end(pos))
			{
				for (int i = 0; i < deep + 1; i++)
					buf_write(data, "  ", 2);
				// key
				json_export_as(data, timezone, pretty, deep + 1, pos);
				buf_write(data, ": ", 2);
				// value
				json_export_as(data, timezone, pretty, deep + 1, pos);
				// ,
				if (data_is_obj_end(*pos))
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
			while (! data_read_obj_end(pos))
			{
				// key
				json_export_as(data, timezone, pretty, deep + 1, pos);
				buf_write(data, ": ", 2);
				// value
				json_export_as(data, timezone, pretty, deep + 1, pos);
				// ,
				if (! data_is_obj_end(*pos))
					buf_write(data, ", ", 2);
			}
			buf_write(data, "}", 1);
		}
		break;
	}
	default:
		error_data();
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
