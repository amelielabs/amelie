
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_data.h>

void
json_export_as(Buf* data, Timezone* timezone, bool pretty, int deep, uint8_t** pos)
{
	switch (**pos) {
	case DATA_NULL:
		unpack_null(pos);
		buf_write(data, "null", 4);
		break;
	case DATA_TRUE:
	case DATA_FALSE:
	{
		bool value;
		unpack_bool(pos, &value);
		if (value)
			buf_write(data, "true", 4);
		else
			buf_write(data, "false", 5);
		break;
	}
	case DATA_REAL32:
	case DATA_REAL64:
	{
		double value;
		unpack_real(pos, &value);
		buf_printf(data, "%g", value);
		break;
	}
	case DATA_INTV0 ... DATA_INT64:
	{
		int64_t value;
		unpack_int(pos, &value);
		buf_printf(data, "%" PRIi64, value);
		break;
	}
	case DATA_STRINGV0 ... DATA_STRING32:
	{
		Str str;
		unpack_string(pos, &str);
		buf_write(data, "\"", 1);
		escape_str(data, &str);
		buf_write(data, "\"", 1);
		break;
	}
	case DATA_ARRAY:
	{
		unpack_array(pos);
		buf_write(data, "[", 1);
		while (! unpack_array_end(pos))
		{
			json_export_as(data, timezone, pretty, deep, pos);
			// ,
			if (! data_is_array_end(*pos))
				buf_write(data, ", ", 2);
		}
		buf_write(data, "]", 1);
		break;
	}
	case DATA_OBJ:
	{
		unpack_obj(pos);
		if (pretty)
		{
			// {}
			if (unpack_obj_end(pos))
			{
				buf_write(data, "{}", 2);
				break;
			}
			buf_write(data, "{\n", 2);
			while (! unpack_obj_end(pos))
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
			while (! unpack_obj_end(pos))
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
		data_error_read();
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
