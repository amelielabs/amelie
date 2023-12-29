
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>

hot int
json_to_string(Buf *data, uint8_t** pos)
{
	int  type = **pos;
	char buf[32];
	int  buf_len;
	switch (type) {
	case MN_NULL:
		data_read_null(pos);
		buf_write(data, "null", 4);
		break;
	case MN_TRUE:
	case MN_FALSE:
	{
		bool value;
		data_read_bool(pos, &value);
		if (value)
			buf_write(data, "true", 4);
		else
			buf_write(data, "false", 5);
		break;
	}
	case MN_REAL32:
	case MN_REAL64:
	{
		double value;
		data_read_real(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%f", value);
		buf_write(data, buf, buf_len);
		break;
	}
	case MN_INTV0 ... MN_INT64:
	{
		int64_t value;
		data_read_integer(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%" PRIi64, value);
		buf_write(data,  buf, buf_len);
		break;
	}
	case MN_STRINGV0 ... MN_STRING32:
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
	case MN_ARRAYV0 ... MN_ARRAY32:
	{
		int value;
		data_read_array(pos, &value);
		buf_write(data, "[", 1);
		while (value-- > 0)
		{
			json_to_string(data, pos);
			if (value > 0)
				buf_write(data, ", ", 2);
		}
		buf_write(data, "]", 1);
		break;
	}
	case MN_MAPV0 ... MN_MAP32:
	{
		int value;
		data_read_map(pos, &value);
		buf_write(data, "{", 1);
		while (value-- > 0)
		{
			// key
			json_to_string(data, pos);
			buf_write(data, ": ", 2);
			// value
			json_to_string(data, pos);
			// ,
			if (value > 0)
				buf_write(data, ", ", 2);
		}
		buf_write(data, "}", 1);
		break;
	}
	default:
	   abort();
	   break;
	}
	return type;
}

hot int
json_to_string_pretty(Buf* data, int deep, uint8_t** pos)
{
	int  type = **pos;
	char buf[32];
	int  buf_len;
	switch (type) {
	case MN_NULL:
		data_read_null(pos);
		buf_write(data, "null", 4);
		break;
	case MN_TRUE:
	case MN_FALSE:
	{
		bool value;
		data_read_bool(pos, &value);
		if (value)
			buf_write(data, "true", 4);
		else
			buf_write(data, "false", 5);
		break;
	}
	case MN_REAL32:
	case MN_REAL64:
	{
		double value;
		data_read_real(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%f", value);
		buf_write(data, buf, buf_len);
		break;
	}
	case MN_INTV0 ... MN_INT64:
	{
		int64_t value;
		data_read_integer(pos, &value);
		buf_len = snprintf(buf, sizeof(buf), "%" PRIi64, value);
		buf_write(data,  buf, buf_len);
		break;
	}
	case MN_STRINGV0 ... MN_STRING32:
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
	case MN_ARRAYV0 ... MN_ARRAY32:
	{
		int value;
		data_read_array(pos, &value);
		buf_write(data, "[", 1);
		while (value-- > 0)
		{
			json_to_string_pretty(data, deep, pos);
			if (value > 0)
				buf_write(data, ", ", 2);
		}
		buf_write(data, "]", 1);
		break;
	}
	case MN_MAPV0 ... MN_MAP32:
	{
		int value;
		data_read_map(pos, &value);
		buf_write(data, "{\n", 2);
		int i;
		while (value-- > 0)
		{
			for (i = 0; i < deep + 1; i++)
				buf_write(data, "  ", 2);
			// key 
			json_to_string_pretty(data, deep + 1, pos);
			buf_write(data, ": ", 2);
			// value
			json_to_string_pretty(data, deep + 1, pos);
			// ,
			if (value > 0)
				buf_write(data, ",\n", 2);
			else
				buf_write(data, "\n", 1);
		}
		for (i = 0; i < deep; i++)
			buf_write(data, "  ", 2);
		buf_write(data, "}", 1);
		break;
	}
	default:
	   abort();
	   break;
	}
	return type;
}
