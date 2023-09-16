#pragma once

//
// monotone
//
// SQL OLTP database
//

always_inline hot static inline void
data_error(uint8_t* data, int type)
{
	error("expected data type '%s', but got '%s'",
	      type_to_string(type),
	      type_to_string(*data));
}

always_inline hot static inline int
data_size_type(void)
{
	return sizeof(uint8_t);
}

always_inline hot static inline int
data_size8(void)
{
	return data_size_type() + sizeof(uint8_t);
}

always_inline hot static inline int
data_size16(void)
{
	return data_size_type() + sizeof(uint16_t);
}

always_inline hot static inline int
data_size32(void)
{
	return data_size_type() + sizeof(uint32_t);
}

always_inline hot static inline int
data_size64(void)
{
	return data_size_type() + sizeof(uint64_t);
}

always_inline hot static inline int
data_size_of(uint64_t value)
{
	if (value <= 31)
		return data_size_type();
	if (value <= INT8_MAX)
		return data_size8();
	if (value <= INT16_MAX)
		return data_size16();
	if (value <= INT32_MAX)
		return data_size32();
	return data_size64();
}

// map
always_inline hot static inline int
data_size_map(int value)
{
	return data_size_of(value);
}

always_inline hot static inline int
data_size_map32(void)
{
	return data_size32();
}

always_inline hot static inline void
data_read_map(uint8_t** pos, int* value)
{
	switch (**pos) {
	case MN_MAPV0 ... MN_MAPV31:
		*value = **pos - MN_MAPV0;
		*pos += data_size_type();
		break;
	case MN_MAP8:
		*value = *(int8_t*)(*pos + data_size_type());
		*pos += data_size8();
		break;
	case MN_MAP16:
		*value = *(int16_t*)(*pos + data_size_type());
		*pos += data_size16();
		break;
	case MN_MAP32:
		*value = *(int32_t*)(*pos + data_size_type());
		*pos += data_size32();
		break;
	default:
		data_error(*pos, MN_MAPV0);
		break;
	}
}

always_inline hot static inline int
data_read_map_at(uint8_t* pos)
{
	int value;
	data_read_map(&pos, &value);
	return value;
}

always_inline hot static inline void
data_write_map(uint8_t** pos, uint32_t value)
{
	uint8_t* data = *pos;
	if (value <= 31)
	{
		*data = MN_MAPV0 + value;
		*pos += data_size_type();
	} else
	if (value <= INT8_MAX)
	{
		*data = MN_MAP8;
		*(int8_t*)(data + data_size_type()) = value;
		*pos += data_size8();
	} else
	if (value <= INT16_MAX)
	{
		*data = MN_MAP16;
		*(int16_t*)(data + data_size_type()) = value;
		*pos += data_size16();
	} else
	{
		*data = MN_MAP32;
		*(int32_t*)(data + data_size_type()) = value;
		*pos += data_size32();
	}
}

always_inline hot static inline void
data_write_map32(uint8_t** pos, uint32_t value)
{
	uint8_t *data = *pos;
	*data = MN_MAP32;
	*(int32_t*)(data + data_size_type()) = value;
	*pos += data_size32();
}

always_inline hot static inline bool
data_is_map(uint8_t* data)
{
	return *data >= MN_MAPV0 && *data <= MN_MAP32;
}

// array
always_inline hot static inline int
data_size_array(int value)
{
	return data_size_of(value);
}

always_inline hot static inline int
data_size_array32(void)
{
	return data_size32();
}

always_inline hot static inline void
data_read_array(uint8_t** pos, int* value)
{
	switch (**pos) {
	case MN_ARRAYV0 ... MN_ARRAYV31:
		*value = **pos - MN_ARRAYV0;
		*pos += data_size_type();
		break;
	case MN_ARRAY8:
		*value = *(int8_t*)(*pos + data_size_type());
		*pos += data_size8();
		break;
	case MN_ARRAY16:
		*value = *(int16_t*)(*pos + data_size_type());
		*pos += data_size16();
		break;
	case MN_ARRAY32:
		*value = *(int32_t*)(*pos + data_size_type());
		*pos += data_size32();
		break;
	default:
		data_error(*pos, MN_ARRAYV0);
		break;
	}
}

always_inline hot static inline int
data_read_array_at(uint8_t* pos)
{
	int value;
	data_read_array(&pos, &value);
	return value;
}

always_inline hot static inline void
data_write_array(uint8_t** pos, uint32_t value)
{
	uint8_t* data = *pos;
	if (value <= 31)
	{
		*data = MN_ARRAYV0 + value;
		*pos += data_size_type();
	} else
	if (value <= INT8_MAX)
	{
		*data = MN_ARRAY8;
		*(int8_t*)(data + data_size_type()) = value;
		*pos += data_size8();
	} else
	if (value <= INT16_MAX)
	{
		*data = MN_ARRAY16;
		*(int16_t*)(data + data_size_type()) = value;
		*pos += data_size16();
	} else
	{
		*data = MN_ARRAY32;
		*(int32_t*)(data + data_size_type()) = value;
		*pos += data_size32();
	}
}

always_inline hot static inline void
data_write_array32(uint8_t** pos, uint32_t value)
{
	uint8_t* data = *pos;
	*data = MN_ARRAY32;
	*(int32_t*)(data + data_size_type()) = value;
	*pos += data_size32();
}

always_inline hot static inline bool
data_is_array(uint8_t* data)
{
	return *data >= MN_ARRAYV0 && *data <= MN_ARRAY32;
}

//  integer
always_inline hot static inline int
data_size_integer(uint64_t value)
{
	return data_size_of(value);
}

always_inline hot static inline void
data_read_integer(uint8_t** pos, int64_t* value)
{
	switch (**pos) {
	case MN_INTV0 ... MN_INTV31:
		*value = **pos - MN_INTV0;
		*pos += data_size_type();
		break;
	case MN_INT8:
		*value = *(int8_t*)(*pos + data_size_type());
		*pos += data_size8();
		break;
	case MN_INT16:
		*value = *(int16_t*)(*pos + data_size_type());
		*pos += data_size16();
		break;
	case MN_INT32:
		*value = *(int32_t*)(*pos + data_size_type());
		*pos += data_size32();
		break;
	case MN_INT64:
		*value = *(int64_t*)(*pos + data_size_type());
		*pos += data_size64();
		break;
	default:
		data_error(*pos, MN_INTV0);
		break;
	}
}

always_inline hot static inline int64_t
data_read_integer_at(uint8_t* pos)
{
	int64_t value;
	data_read_integer(&pos, &value);
	return value;
}

always_inline hot static inline void
data_write_integer(uint8_t** pos, uint64_t value)
{
	uint8_t* data = *pos;
	if (value <= 31)
	{
		*data = MN_INTV0 + value;
		*pos += data_size_type();
	} else
	if (value <= INT8_MAX)
	{
		*data = MN_INT8;
		*(int8_t*)(data + data_size_type()) = value;
		*pos += data_size8();
	} else
	if (value <= INT16_MAX)
	{
		*data = MN_INT16;
		*(int16_t*)(data + data_size_type()) = value;
		*pos += data_size16();
	} else
	if (value <= INT32_MAX)
	{
		*data = MN_INT32;
		*(int32_t*)(data + data_size_type()) = value;
		*pos += data_size32();
	} else
	{
		*data = MN_INT64;
		*(int64_t*)(data + data_size_type()) = value;
		*pos += data_size64();
	}
}

always_inline hot static inline bool
data_is_integer(uint8_t* data)
{
	return *data >= MN_INTV0 && *data <= MN_INT64;
}

always_inline hot static inline int
data_compare_integer_read(uint8_t** a, uint8_t** b)
{
	int64_t a_value;
	int64_t b_value;
	data_read_integer(a, &a_value);
	data_read_integer(b, &b_value);
	if (a_value == b_value)
		return 0;
	return (a_value > b_value) ? 1 : -1;
}

always_inline hot static inline int
data_compare_integer(uint8_t* a, uint8_t* b)
{
	return data_compare_integer_read(&a, &b);
}

// string
always_inline hot static inline int
data_size_string(int value)
{
	return data_size_of(value) + value;
}

always_inline hot static inline int
data_size_string32(void)
{
	return data_size32();
}

always_inline hot static inline void
data_read_raw(uint8_t** pos, char** value, int* size)
{
	switch (**pos) {
	case MN_STRINGV0 ... MN_STRINGV31:
		*size = **pos - MN_STRINGV0;
		*pos += data_size_type();
		break;
	case MN_STRING8:
		*size = *(int8_t*)(*pos + data_size_type());
		*pos += data_size8();
		break;
	case MN_STRING16:
		*size = *(int16_t*)(*pos + data_size_type());
		*pos += data_size16();
		break;
	case MN_STRING32:
		*size = *(int32_t*)(*pos + data_size_type());
		*pos += data_size32();
		break;
	default:
		data_error(*pos, MN_STRINGV0);
		break;
	}
	*value = *(char**)pos;
	*pos += *size;
}

always_inline hot static inline void
data_read_string(uint8_t** pos, Str* string)
{
	char* value;
	int   value_size;
	data_read_raw(pos, &value, &value_size);
	str_set(string, value, value_size);
}

always_inline hot static inline void
data_read_string_copy(uint8_t** pos, Str* string)
{
	char* value;
	int   value_size;
	data_read_raw(pos, &value, &value_size);
	str_strndup(string, value, value_size);
}

always_inline hot static inline void
data_write_raw(uint8_t** pos, const char* value, uint32_t size)
{
	uint8_t* data = *pos;
	if (size <= 31)
	{
		*data = MN_STRINGV0 + size;
		*pos += data_size_type();
	} else
	if (size <= INT8_MAX)
	{
		*data = MN_STRING8;
		*(int8_t*)(data + data_size_type()) = size;
		*pos += data_size8();
	} else
	if (size <= INT16_MAX)
	{
		*data = MN_STRING16;
		*(int16_t*)(data + data_size_type()) = size;
		*pos += data_size16();
	} else
	{
		*data = MN_STRING32;
		*(int32_t*)(data + data_size_type()) = size;
		*pos += data_size32();
	}
	if (size > 0)
		memcpy(*pos, value, size);
	*pos += size;
}

always_inline hot static inline void
data_write_string(uint8_t** pos, Str* string)
{
	data_write_raw(pos, str_of(string), str_size(string));
}

always_inline hot static inline void
data_write_string_cat(uint8_t** pos, char* a, char* b, int a_size, int b_size)
{
	uint8_t* data = *pos;
	uint32_t size = a_size + b_size;
	if (size <= 31)
	{
		*data = MN_STRINGV0 + size;
		*pos += data_size_type();
	} else
	if (size <= INT8_MAX)
	{
		*data = MN_STRING8;
		*(int8_t*)(data + data_size_type()) = size;
		*pos += data_size8();
	} else
	if (size <= INT16_MAX)
	{
		*data = MN_STRING16;
		*(int16_t*)(data + data_size_type()) = size;
		*pos += data_size16();
	} else
	if (size <= INT32_MAX)
	{
		*data = MN_STRING32;
		*(int32_t*)(data + data_size_type()) = size;
		*pos += data_size32();
	}
	memcpy(*pos, a, a_size);
	*pos += a_size;
	memcpy(*pos, b, b_size);
	*pos += b_size;
}

always_inline hot static inline void
data_write_string32(uint8_t** pos, uint32_t size)
{
	uint8_t* data = *pos;
	*data = MN_STRING32;
	*(int32_t*)(data + data_size_type()) = size;
	*pos += data_size32();
}

always_inline hot static inline bool
data_is_string(uint8_t* data)
{
	return *data >= MN_STRINGV0 && *data <= MN_STRING32;
}

always_inline hot static inline int
data_compare_string_read(uint8_t** a, uint8_t** b)
{
	char* a_value;
	int   a_value_size;
	char* b_value;
	int   b_value_size;
	data_read_raw(a, &a_value, &a_value_size);
	data_read_raw(b, &b_value, &b_value_size);
	int size;
	if (a_value_size < b_value_size)
		size = a_value_size;
	else
		size = b_value_size;
	int rc;
	rc = memcmp(a_value, b_value, size);
	if (rc == 0) {
		if (likely(a_value_size == b_value_size))
			return 0;
		return (a_value_size < b_value_size) ? -1 : 1;
	}
	return rc > 0 ? 1 : -1;
}

always_inline hot static inline int
data_compare_string(uint8_t* a, uint8_t* b)
{
	return data_compare_string_read(&a, &b);
}

// bool
always_inline hot static inline int
data_size_bool(void)
{
	return data_size_type();
}

always_inline hot static inline void
data_read_bool(uint8_t** pos, bool* value)
{
	uint8_t* data = *pos;
	if (*data == MN_TRUE)
		*value = true;
	else
	if (*data == MN_FALSE)
		*value = false;
	else
		data_error(*pos, MN_FALSE);
	*pos += data_size_bool();
}

always_inline hot static inline bool
data_read_bool_at(uint8_t* pos)
{
	bool value;
	data_read_bool(&pos, &value);
	return value;
}

always_inline hot static inline void
data_write_bool(uint8_t** pos, bool value)
{
	uint8_t* data = *pos;
	if (value)
		*data = MN_TRUE;
	else
		*data = MN_FALSE;
	*pos += data_size_bool();
}

always_inline hot static inline bool
data_is_bool(uint8_t* data)
{
	return *data == MN_TRUE || *data == MN_FALSE;
}

always_inline hot static inline int
data_compare_bool(uint8_t* a, uint8_t* b)
{
	int a_value;
	int b_value;
	a_value = data_read_bool_at(a);
	b_value = data_read_bool_at(b);
	if (a_value == b_value)
		return 0;
	return (a_value > b_value) ? 1 : -1;
}

// float
always_inline hot static inline int
data_size_float(void)
{
	return data_size_type() + sizeof(float);
}

always_inline hot static inline void
data_read_float(uint8_t** pos, float* value)
{
	uint8_t* data = *pos;
	if (unlikely(*data != MN_FLOAT))
		data_error(*pos, MN_FLOAT);
	*value = *(float*)(data + data_size_type());
	*pos += data_size_float();
}

always_inline hot static inline float
data_read_float_at(uint8_t* pos)
{
	float value;
	data_read_float(&pos, &value);
	return value;
}

always_inline hot static inline void
data_write_float(uint8_t** pos, float value)
{
	uint8_t* data = *pos;
	*data = MN_FLOAT;
	*(float*)(data + data_size_type()) = value;
	*pos += data_size_float();
}

always_inline hot static inline bool
data_is_float(uint8_t* data)
{
	return *data == MN_FLOAT;
}

always_inline hot static inline int
data_compare_float(uint8_t* a, uint8_t* b)
{
	float a_value;
	float b_value;
	a_value = data_read_float_at(a);
	b_value = data_read_float_at(b);
	if (a_value == b_value)
		return 0;
	return (a_value > b_value) ? 1 : -1;
}

// null
always_inline hot static inline int
data_size_null(void)
{
	return data_size_type();
}

always_inline hot static inline void
data_read_null(uint8_t** pos)
{
	if (unlikely(**pos != MN_NULL))
		data_error(*pos, MN_NULL);
	*pos += data_size_type();
}

always_inline hot static inline void
data_write_null(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = MN_NULL;
	*pos += data_size_type();
}

always_inline hot static inline bool
data_is_null(uint8_t* data)
{
	return *data == MN_NULL;
}

// partial
always_inline hot static inline int
data_size_partial(void)
{
	return data_size_type();
}

always_inline hot static inline void
data_read_partial(uint8_t** pos, int* type)
{
	if (unlikely(! (**pos >= MN_PARTIAL && **pos <= MN_PARTIALMAX)))
		data_error(*pos, MN_PARTIAL);
	*type = **pos - MN_PARTIAL;
	*pos += data_size_type();
}

always_inline hot static inline int
data_read_partial_at(uint8_t* pos)
{
	if (unlikely(! (*pos >= MN_PARTIAL && *pos <= MN_PARTIALMAX)))
		data_error(pos, MN_PARTIAL);
	return *pos - MN_PARTIAL;
}

always_inline hot static inline void
data_write_partial(uint8_t** pos, int type)
{
	uint8_t* data = *pos;
	*data = MN_PARTIAL + type;
	*pos += data_size_type();
}

always_inline hot static inline bool
data_is_partial(uint8_t* data)
{
	return *data >= MN_PARTIAL && *data <= MN_PARTIALMAX;
}

// misc
hot static inline void
data_skip(uint8_t** pos)
{
	switch (**pos) {
	case MN_TRUE:
	case MN_FALSE:
	{
		bool value;
		data_read_bool(pos, &value);
		break;
	}
	case MN_NULL:
	{
		data_read_null(pos);
		break;
	}
	case MN_FLOAT:
	{
		float value;
		data_read_float(pos, &value);
		break;
	}
	case MN_INTV0 ... MN_INT64:
	{
		int64_t value;
		data_read_integer(pos, &value);
		break;
	}
	case MN_ARRAYV0 ... MN_ARRAY32:
	{
		int count;
		data_read_array(pos, &count);
		while (count-- > 0)
			data_skip(pos);
		break;
	}
	case MN_MAPV0 ... MN_MAP32:
	{
		int count;
		data_read_map(pos, &count);
		while (count-- > 0)
		{
			data_skip(pos);
			data_skip(pos);
		}
		break;
	}
	case MN_STRINGV0 ... MN_STRING32:
	{
		int   value_size;
		char* value;
		data_read_raw(pos, &value, &value_size);
		break;
	}
	case MN_PARTIAL ... MN_PARTIALMAX:
	{
		int type;
		data_read_partial(pos, &type);
		data_skip(pos);
		break;
	}
	default:
		error_data();
		break;
	}
}
