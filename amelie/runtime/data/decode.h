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

typedef struct Decode Decode;

enum
{
	DECODE_UUID     = 1 << 0,
	DECODE_STR      = 1 << 1,
	DECODE_STR_READ = 1 << 2,
	DECODE_INT      = 1 << 3,
	DECODE_BOOL     = 1 << 4,
	DECODE_REAL     = 1 << 5,
	DECODE_NULL     = 1 << 6,
	DECODE_ARRAY    = 1 << 7,
	DECODE_OBJ      = 1 << 8,
	DECODE_DATA     = 1 << 9,
	DECODE_BASE64   = 1 << 10,
	DECODE_FOUND    = 1 << 11
};

struct Decode
{
	int         flags;
	const char* key;
	void*       value;
};

static inline void
decode_obj(Decode* self, const char* context, uint8_t** pos)
{
	// read obj and compare against keys
	unpack_obj(pos);
	while (! unpack_obj_end(pos))
	{
		Str key;
		unpack_str(pos, &key);

		bool found = false;
		for (auto ref = self; ref->key; ref++)
		{
			if (! str_is_cstr(&key, ref->key))
				continue;

			switch (ref->flags & ~DECODE_FOUND) {
			case DECODE_UUID:
			{
				if (unlikely(! data_is_str(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				auto value = (Uuid*)ref->value;
				Str uuid;
				unpack_str(pos, &uuid);
				uuid_set(value, &uuid);
				break;
			}
			case DECODE_STR:
			{
				if (unlikely(! data_is_str(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				auto value = (Str*)ref->value;
				str_free(value);
				unpack_str_copy(pos, value);
				break;
			}
			case DECODE_STR_READ:
			{
				if (unlikely(! data_is_str(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				auto value = (Str*)ref->value;
				unpack_str(pos, value);
				break;
			}
			case DECODE_INT:
			{
				if (unlikely(! data_is_int(*pos)))
					error("%s: int expected for '%s'", context,
					      ref->key);
				auto value = (int64_t*)ref->value;
				unpack_int(pos, value);
				break;
			}
			case DECODE_BOOL:
			{
				if (unlikely(! data_is_bool(*pos)))
					error("%s: bool expected for '%s'", context,
					      ref->key);
				auto value = (bool*)ref->value;
				unpack_bool(pos, value);
				break;
			}
			case DECODE_REAL:
			{
				if (unlikely(! data_is_bool(*pos)))
					error("%s: real expected for '%s'", context,
					      ref->key);
				auto value = (double*)ref->value;
				unpack_real(pos, value);
				break;
			}
			case DECODE_NULL:
			{
				if (unlikely(! data_is_bool(*pos)))
					error("%s: null expected for '%s'", context,
					      ref->key);
				unpack_null(pos);
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
			case DECODE_OBJ:
			{
				if (unlikely(! data_is_obj(*pos)))
					error("%s: object expected for '%s'", context,
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
			case DECODE_BASE64:
			{
				if (unlikely(! data_is_str(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				auto value = (Buf*)ref->value;
				Str str;
				unpack_str(pos, &str);
				base64url_decode(value, &str);
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
