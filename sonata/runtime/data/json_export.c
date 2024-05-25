
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>

static void
json_export_as(Buf* data, bool pretty, int deep, uint8_t** pos)
{
	char buf[256];
	int  buf_len;
	switch (**pos) {
	case SO_NULL:
		data_read_null(pos);
		buf_write(data, "null", 4);
		break;
	case SO_TRUE:
	case SO_FALSE:
	{
		bool value;
		data_read_bool(pos, &value);
		if (value)
			buf_write(data, "true", 4);
		else
			buf_write(data, "false", 5);
		break;
	}
	case SO_REAL32:
	case SO_REAL64:
	{
		double value;
		data_read_real(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%g", value);
		buf_write(data, buf, buf_len);
		break;
	}
	case SO_INTV0 ... SO_INT64:
	{
		int64_t value;
		data_read_integer(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%" PRIi64, value);
		buf_write(data, buf, buf_len);
		break;
	}
	case SO_STRINGV0 ... SO_STRING32:
	{
		// todo: quouting
		char* value;
		int   size;
		data_read_raw(pos, &value, &size);
		buf_write(data, "\"", 1);
		buf_write(data, value, size);
		buf_write(data, "\"", 1);
		break;
	}
	case SO_ARRAY:
	{
		data_read_array(pos);
		buf_write(data, "[", 1);
		while (! data_read_array_end(pos))
		{
			json_export_as(data, pretty, deep, pos);
			// ,
			if (! data_is_array_end(*pos))
				buf_write(data, ", ", 2);
		}
		buf_write(data, "]", 1);
		break;
	}
	case SO_MAP:
	{
		data_read_map(pos);
		if (pretty)
		{
			buf_write(data, "{\n", 2);
			while (! data_read_map_end(pos))
			{
				for (int i = 0; i < deep + 1; i++)
					buf_write(data, "  ", 2);
				// key
				json_export_as(data, pretty, deep + 1, pos);
				buf_write(data, ": ", 2);
				// value
				json_export_as(data, pretty, deep + 1, pos);
				// ,
				if (data_is_map_end(*pos))
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
			while (! data_read_map_end(pos))
			{
				// key
				json_export_as(data, pretty, deep + 1, pos);
				buf_write(data, ": ", 2);
				// value
				json_export_as(data, pretty, deep + 1, pos);
				// ,
				if (! data_is_map_end(*pos))
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
json_export(Buf* self, uint8_t** pos)
{
	json_export_as(self, false, 0, pos);
}

void
json_export_pretty(Buf* self, uint8_t** pos)
{
	json_export_as(self, true, 0, pos);
}
