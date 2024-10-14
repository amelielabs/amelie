#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
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
	case AM_INTV0 ... AM_INTV31:
		*value = **pos - AM_INTV0;
		*pos += data_size_type();
		break;
	case AM_INT8:
		*value = *(int8_t*)(*pos + data_size_type());
		*pos += data_size8();
		break;
	case AM_INT16:
		*value = *(int16_t*)(*pos + data_size_type());
		*pos += data_size16();
		break;
	case AM_INT32:
		*value = *(int32_t*)(*pos + data_size_type());
		*pos += data_size32();
		break;
	case AM_INT64:
		*value = *(int64_t*)(*pos + data_size_type());
		*pos += data_size64();
		break;
	default:
		data_error(*pos, AM_INTV0);
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
		*data = AM_INTV0 + value;
		*pos += data_size_type();
	} else
	if (value <= INT8_MAX)
	{
		*data = AM_INT8;
		*(int8_t*)(data + data_size_type()) = value;
		*pos += data_size8();
	} else
	if (value <= INT16_MAX)
	{
		*data = AM_INT16;
		*(int16_t*)(data + data_size_type()) = value;
		*pos += data_size16();
	} else
	if (value <= INT32_MAX)
	{
		*data = AM_INT32;
		*(int32_t*)(data + data_size_type()) = value;
		*pos += data_size32();
	} else
	{
		*data = AM_INT64;
		*(int64_t*)(data + data_size_type()) = value;
		*pos += data_size64();
	}
}

always_inline hot static inline bool
data_is_integer(uint8_t* data)
{
	return *data >= AM_INTV0 && *data <= AM_INT64;
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
	case AM_STRINGV0 ... AM_STRINGV31:
		*size = **pos - AM_STRINGV0;
		*pos += data_size_type();
		break;
	case AM_STRING8:
		*size = *(int8_t*)(*pos + data_size_type());
		*pos += data_size8();
		break;
	case AM_STRING16:
		*size = *(int16_t*)(*pos + data_size_type());
		*pos += data_size16();
		break;
	case AM_STRING32:
		*size = *(int32_t*)(*pos + data_size_type());
		*pos += data_size32();
		break;
	default:
		data_error(*pos, AM_STRINGV0);
		break;
	}
	*value = (char*)*pos;
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
		*data = AM_STRINGV0 + size;
		*pos += data_size_type();
	} else
	if (size <= INT8_MAX)
	{
		*data = AM_STRING8;
		*(int8_t*)(data + data_size_type()) = size;
		*pos += data_size8();
	} else
	if (size <= INT16_MAX)
	{
		*data = AM_STRING16;
		*(int16_t*)(data + data_size_type()) = size;
		*pos += data_size16();
	} else
	{
		*data = AM_STRING32;
		*(int32_t*)(data + data_size_type()) = size;
		*pos += data_size32();
	}
	if (value && size > 0)
	{
		memcpy(*pos, value, size);
		*pos += size;
	}
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
		*data = AM_STRINGV0 + size;
		*pos += data_size_type();
	} else
	if (size <= INT8_MAX)
	{
		*data = AM_STRING8;
		*(int8_t*)(data + data_size_type()) = size;
		*pos += data_size8();
	} else
	if (size <= INT16_MAX)
	{
		*data = AM_STRING16;
		*(int16_t*)(data + data_size_type()) = size;
		*pos += data_size16();
	} else
	if (size <= INT32_MAX)
	{
		*data = AM_STRING32;
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
	*data = AM_STRING32;
	*(int32_t*)(data + data_size_type()) = size;
	*pos += data_size32();
}

always_inline hot static inline bool
data_is_string(uint8_t* data)
{
	return *data >= AM_STRINGV0 && *data <= AM_STRING32;
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
	if (*data == AM_TRUE)
		*value = true;
	else
	if (*data == AM_FALSE)
		*value = false;
	else
		data_error(*pos, AM_FALSE);
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
		*data = AM_TRUE;
	else
		*data = AM_FALSE;
	*pos += data_size_bool();
}

always_inline hot static inline bool
data_is_bool(uint8_t* data)
{
	return *data == AM_TRUE || *data == AM_FALSE;
}

// real
always_inline hot static inline int
data_size_real(double value)
{
	if (value >= FLT_MIN && value <= FLT_MAX)
		return data_size_type() + sizeof(float);
	return data_size_type() + sizeof(double);
}
always_inline hot static inline void
data_read_real(uint8_t** pos, double* value)
{
	uint8_t* data = *pos;
	if (*data == AM_REAL32)
	{
		*value = *(float*)(data + data_size_type());
		*pos += data_size_type() + sizeof(float);
	} else
	if (*data == AM_REAL64)
	{
		*value = *(double*)(data + data_size_type());
		*pos += data_size_type() + sizeof(double);
	} else {
		data_error(*pos, AM_REAL32);
	}
}

always_inline hot static inline double
data_read_real_at(uint8_t* pos)
{
	double value;
	data_read_real(&pos, &value);
	return value;
}

always_inline hot static inline void
data_write_real(uint8_t** pos, double value)
{
	uint8_t* data = *pos;
	if (value >= FLT_MIN && value <= FLT_MAX)
	{
		*data = AM_REAL32;
		*(float*)(data + data_size_type()) = value;
		*pos += data_size_type() + sizeof(float);
	} else
	{
		*data = AM_REAL64;
		*(double*)(data + data_size_type()) = value;
		*pos += data_size_type() + sizeof(double);
	}
}

always_inline hot static inline bool
data_is_real(uint8_t* data)
{
	return *data == AM_REAL32 || *data == AM_REAL64;
}

// obj
always_inline hot static inline int
data_size_obj(void)
{
	return data_size_type();
}

always_inline hot static inline int
data_size_obj_end(void)
{
	return data_size_type();
}

always_inline hot static inline void
data_read_obj(uint8_t** pos)
{
	if (unlikely(**pos != AM_OBJ))
		data_error(*pos, AM_OBJ);
	*pos += data_size_type();
}

always_inline hot static inline bool
data_read_obj_end(uint8_t** pos)
{
	if (unlikely(**pos != AM_OBJ_END))
		return false;
	*pos += data_size_type();
	return true;
}

always_inline hot static inline void
data_write_obj(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = AM_OBJ;
	*pos += data_size_type();
}

always_inline hot static inline void
data_write_obj_end(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = AM_OBJ_END;
	*pos += data_size_type();
}

always_inline hot static inline bool
data_is_obj(uint8_t* data)
{
	return *data == AM_OBJ;
}

always_inline hot static inline bool
data_is_obj_end(uint8_t* data)
{
	return *data == AM_OBJ_END;
}

// array
always_inline hot static inline int
data_size_array(void)
{
	return data_size_type();
}

always_inline hot static inline int
data_size_array_end(void)
{
	return data_size_type();
}

always_inline hot static inline void
data_read_array(uint8_t** pos)
{
	if (unlikely(**pos != AM_ARRAY))
		data_error(*pos, AM_ARRAY);
	*pos += data_size_type();
}

always_inline hot static inline bool
data_read_array_end(uint8_t** pos)
{
	if (unlikely(**pos != AM_ARRAY_END))
		return false;
	*pos += data_size_type();
	return true;
}

always_inline hot static inline void
data_write_array(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = AM_ARRAY;
	*pos += data_size_type();
}

always_inline hot static inline void
data_write_array_end(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = AM_ARRAY_END;
	*pos += data_size_type();
}

always_inline hot static inline bool
data_is_array(uint8_t* data)
{
	return *data == AM_ARRAY;
}

always_inline hot static inline bool
data_is_array_end(uint8_t* data)
{
	return *data == AM_ARRAY_END;
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
	if (unlikely(**pos != AM_NULL))
		data_error(*pos, AM_NULL);
	*pos += data_size_type();
}

always_inline hot static inline void
data_write_null(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = AM_NULL;
	*pos += data_size_type();
}

always_inline hot static inline bool
data_is_null(uint8_t* data)
{
	return *data == AM_NULL;
}
