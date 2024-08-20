#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Decode Decode;

enum
{
	DECODE_UUID            = 1 << 0,
	DECODE_STRING          = 1 << 1,
	DECODE_STRING_READ     = 1 << 2,
	DECODE_INT             = 1 << 3,
	DECODE_BOOL            = 1 << 4,
	DECODE_REAL            = 1 << 5,
	DECODE_NULL            = 1 << 6,
	DECODE_ARRAY           = 1 << 7,
	DECODE_MAP             = 1 << 8,
	DECODE_DATA            = 1 << 9,
	DECODE_INTERVAL        = 1 << 10,
	DECODE_INTERVAL_STRING = 1 << 11,
	DECODE_TS              = 1 << 12,
	DECODE_TS_STRING       = 1 << 13,
	DECODE_FOUND           = 1 << 14
};

struct Decode
{
	int         flags;
	const char* key;
	void*       value;
};

static inline void
decode_map(Decode* self, const char* context, uint8_t** pos)
{
	// read map and compare against keys
	data_read_map(pos);
	while (! data_read_map_end(pos))
	{
		Str key;
		data_read_string(pos, &key);

		bool found = false;
		for (auto ref = self; ref->key; ref++)
		{
			if (! str_compare_cstr(&key, ref->key))
				continue;

			switch (ref->flags & ~DECODE_FOUND) {
			case DECODE_INTERVAL:
			{
				if (unlikely(! data_is_interval(*pos)))
					error("%s: interval expected for '%s'", context,
					      ref->key);
				auto value = (Interval*)ref->value;
				data_read_interval(pos, value);
				break;
			}
			case DECODE_INTERVAL_STRING:
			{
				if (unlikely(! data_is_string(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				Str str;
				data_read_string(pos, &str);
				auto value = (Interval*)ref->value;
				interval_read(value, &str);
				break;
			}
			case DECODE_TS:
			{
				if (unlikely(! data_is_timestamp(*pos)))
					error("%s: timestamp expected for '%s'",
					      context, ref->key);
				auto value = (int64_t*)ref->value;
				data_read_timestamp(pos, value);
				break;
			}
			case DECODE_TS_STRING:
			{
				if (unlikely(! data_is_string(*pos)))
					error("%s: string expected for '%s'", context,
					       ref->key);
				Str str;
				data_read_string(pos, &str);
				auto value = (Timestamp*)ref->value;
				timestamp_read(value, &str);
				break;
			}
			case DECODE_UUID:
			{
				if (unlikely(! data_is_string(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				auto value = (Uuid*)ref->value;
				Str uuid;
				data_read_string(pos, &uuid);
				uuid_from_string(value, &uuid);
				break;
			}
			case DECODE_STRING:
			{
				if (unlikely(! data_is_string(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				auto value = (Str*)ref->value;
				str_free(value);
				data_read_string_copy(pos, value);
				break;
			}
			case DECODE_STRING_READ:
			{
				if (unlikely(! data_is_string(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				auto value = (Str*)ref->value;
				data_read_string(pos, value);
				break;
			}
			case DECODE_INT:
			{
				if (unlikely(! data_is_integer(*pos)))
					error("%s: integer expected for '%s'", context,
					      ref->key);
				auto value = (int64_t*)ref->value;
				data_read_integer(pos, value);
				break;
			}
			case DECODE_BOOL:
			{
				if (unlikely(! data_is_bool(*pos)))
					error("%s: bool expected for '%s'", context,
					      ref->key);
				auto value = (bool*)ref->value;
				data_read_bool(pos, value);
				break;
			}
			case DECODE_REAL:
			{
				if (unlikely(! data_is_bool(*pos)))
					error("%s: real expected for '%s'", context,
					      ref->key);
				auto value = (double*)ref->value;
				data_read_real(pos, value);
				break;
			}
			case DECODE_NULL:
			{
				if (unlikely(! data_is_bool(*pos)))
					error("%s: null expected for '%s'", context,
					      ref->key);
				data_read_null(pos);
				break;
			}
			case DECODE_ARRAY:
			{
				if (unlikely(! data_is_array(*pos)))
					error("%s: array expected for '%s'", context,
					      ref->key);
				auto value = (uint8_t**)ref->value;
				*value = *pos;
				data_skip(pos);
				break;
			}
			case DECODE_MAP:
			{
				if (unlikely(! data_is_map(*pos)))
					error("%s: map expected for '%s'", context,
					      ref->key);
				auto value = (uint8_t**)ref->value;
				*value = *pos;
				data_skip(pos);
				break;
			}
			case DECODE_DATA:
			{
				auto value = (Buf*)ref->value;
				auto start = *pos;
				data_skip(pos);
				buf_write(value, start, *pos - start);
				break;
			}
			default:
				abort();
				break;
			}

			ref->flags |= DECODE_FOUND;
			found = true;
			break;
		}

		if (! found)
			data_skip(pos);
	}

	// ensure all keys were found
	for (auto ref = self; ref->key; ref++)
	{
		if (! (ref->flags & DECODE_FOUND))
			error("%s: key '%s' is not found", context, ref->key);
	}
}
