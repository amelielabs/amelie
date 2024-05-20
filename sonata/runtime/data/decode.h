#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Decode Decode;

enum
{
	DECODE_UUID   = 1 << 0,
	DECODE_STRING = 1 << 1,
	DECODE_INT    = 1 << 2,
	DECODE_BOOL   = 1 << 3,
	DECODE_REAL   = 1 << 4,
	DECODE_NULL   = 1 << 5,
	DECODE_ARRAY  = 1 << 6,
	DECODE_MAP    = 1 << 7,
	DECODE_FOUND  = 1 << 8
};

struct Decode
{
	int         flags;
	const char* key;
	void*       value;
};

static inline void
decode_map(Decode* self, uint8_t** pos)
{
	// read map and compare against keys
	int count;
	data_read_map(pos, &count);
	while (count-- > 0)
	{
		Str key;
		data_read_string(pos, &key);

		bool found = false;
		for (auto ref = self; ref->key; ref++)
		{
			if (! str_compare_cstr(&key, ref->key))
				continue;

			switch (ref->flags & ~DECODE_FOUND) {
			case DECODE_UUID:
			{
				if (unlikely(! data_is_string(*pos)))
					error("config: string expected for '%s'", ref->key);
				auto value = (Uuid*)ref->value;
				Str uuid;
				data_read_string(pos, &uuid);
				uuid_from_string(value, &uuid);
				break;
			}
			case DECODE_STRING:
			{
				if (unlikely(! data_is_string(*pos)))
					error("config: string expected for '%s'", ref->key);
				auto value = (Str*)ref->value;
				str_free(value);
				data_read_string_copy(pos, value);
				break;
			}
			case DECODE_INT:
			{
				if (unlikely(! data_is_integer(*pos)))
					error("config: integer expected for '%s'", ref->key);
				auto value = (int64_t*)ref->value;
				data_read_integer(pos, value);
				break;
			}
			case DECODE_BOOL:
			{
				if (unlikely(! data_is_bool(*pos)))
					error("config: bool expected for '%s'", ref->key);
				auto value = (bool*)ref->value;
				data_read_bool(pos, value);
				break;
			}
			case DECODE_REAL:
			{
				if (unlikely(! data_is_bool(*pos)))
					error("config: real expected for '%s'", ref->key);
				auto value = (double*)ref->value;
				data_read_real(pos, value);
				break;
			}
			case DECODE_NULL:
			{
				if (unlikely(! data_is_bool(*pos)))
					error("config: null expected for '%s'", ref->key);
				data_read_null(pos);
				break;
			}
			case DECODE_ARRAY:
			{
				if (unlikely(! data_is_array(*pos)))
					error("config: array expected for '%s'", ref->key);
				auto value = (uint8_t**)ref->value;
				*value = *pos;
				data_skip(pos);
				break;
			}
			case DECODE_MAP:
			{
				if (unlikely(! data_is_map(*pos)))
					error("config: map expected for '%s'", ref->key);
				auto value = (uint8_t**)ref->value;
				*value = *pos;
				data_skip(pos);
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
		{
			data_skip(pos);
			log("config: unknown key '%.*s'", str_size(&key),
			    str_of(&key));
		}
	}

	// ensure all keys were found
	for (auto ref = self; ref->key; ref++)
	{
		if (! (ref->flags & DECODE_FOUND))
			error("config: key '%s' is not found", ref->key);
	}
}
