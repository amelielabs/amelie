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
	DECODE_UUID        = 1 << 0,
	DECODE_STRING      = 1 << 1,
	DECODE_STRING_READ = 1 << 2,
	DECODE_INT         = 1 << 3,
	DECODE_BOOL        = 1 << 4,
	DECODE_REAL        = 1 << 5,
	DECODE_NULL        = 1 << 6,
	DECODE_ARRAY       = 1 << 7,
	DECODE_OBJ         = 1 << 8,
	DECODE_DATA        = 1 << 9,
	DECODE_FOUND       = 1 << 10
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
	json_read_obj(pos);
	while (! json_read_obj_end(pos))
	{
		Str key;
		json_read_string(pos, &key);

		bool found = false;
		for (auto ref = self; ref->key; ref++)
		{
			if (! str_is_cstr(&key, ref->key))
				continue;

			switch (ref->flags & ~DECODE_FOUND) {
			case DECODE_UUID:
			{
				if (unlikely(! json_is_string(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				auto value = (Uuid*)ref->value;
				Str uuid;
				json_read_string(pos, &uuid);
				uuid_set(value, &uuid);
				break;
			}
			case DECODE_STRING:
			{
				if (unlikely(! json_is_string(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				auto value = (Str*)ref->value;
				str_free(value);
				json_read_string_copy(pos, value);
				break;
			}
			case DECODE_STRING_READ:
			{
				if (unlikely(! json_is_string(*pos)))
					error("%s: string expected for '%s'", context,
					      ref->key);
				auto value = (Str*)ref->value;
				json_read_string(pos, value);
				break;
			}
			case DECODE_INT:
			{
				if (unlikely(! json_is_integer(*pos)))
					error("%s: integer expected for '%s'", context,
					      ref->key);
				auto value = (int64_t*)ref->value;
				json_read_integer(pos, value);
				break;
			}
			case DECODE_BOOL:
			{
				if (unlikely(! json_is_bool(*pos)))
					error("%s: bool expected for '%s'", context,
					      ref->key);
				auto value = (bool*)ref->value;
				json_read_bool(pos, value);
				break;
			}
			case DECODE_REAL:
			{
				if (unlikely(! json_is_bool(*pos)))
					error("%s: real expected for '%s'", context,
					      ref->key);
				auto value = (double*)ref->value;
				json_read_real(pos, value);
				break;
			}
			case DECODE_NULL:
			{
				if (unlikely(! json_is_bool(*pos)))
					error("%s: null expected for '%s'", context,
					      ref->key);
				json_read_null(pos);
				break;
			}
			case DECODE_ARRAY:
			{
				if (unlikely(! json_is_array(*pos)))
					error("%s: array expected for '%s'", context,
					      ref->key);
				auto value = (uint8_t**)ref->value;
				*value = *pos;
				json_skip(pos);
				break;
			}
			case DECODE_OBJ:
			{
				if (unlikely(! json_is_obj(*pos)))
					error("%s: object expected for '%s'", context,
					      ref->key);
				auto value = (uint8_t**)ref->value;
				*value = *pos;
				json_skip(pos);
				break;
			}
			case DECODE_DATA:
			{
				auto value = (Buf*)ref->value;
				auto start = *pos;
				json_skip(pos);
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
			json_skip(pos);
	}

	// ensure all keys were found
	for (auto ref = self; ref->key; ref++)
	{
		if (! (ref->flags & DECODE_FOUND))
			error("%s: key '%s' is not found", context, ref->key);
	}
}
