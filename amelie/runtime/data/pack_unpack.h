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

// int
always_inline hot static inline void
pack_int(uint8_t** pos, uint64_t value)
{
	uint8_t* data = *pos;
	if (value <= 31)
	{
		*data = DATA_INTV0 + value;
		*pos += data_size_type();
	} else
	if (value <= INT8_MAX)
	{
		*data = DATA_INT8;
		*(int8_t*)(data + data_size_type()) = value;
		*pos += data_size8();
	} else
	if (value <= INT16_MAX)
	{
		*data = DATA_INT16;
		*(int16_t*)(data + data_size_type()) = value;
		*pos += data_size16();
	} else
	if (value <= INT32_MAX)
	{
		*data = DATA_INT32;
		*(int32_t*)(data + data_size_type()) = value;
		*pos += data_size32();
	} else
	{
		*data = DATA_INT64;
		*(int64_t*)(data + data_size_type()) = value;
		*pos += data_size64();
	}
}

always_inline hot static inline void
unpack_int(uint8_t** pos, int64_t* value)
{
	switch (**pos) {
	case DATA_INTV0 ... DATA_INTV31:
		*value = **pos - DATA_INTV0;
		*pos += data_size_type();
		break;
	case DATA_INT8:
		*value = *(int8_t*)(*pos + data_size_type());
		*pos += data_size8();
		break;
	case DATA_INT16:
		*value = *(int16_t*)(*pos + data_size_type());
		*pos += data_size16();
		break;
	case DATA_INT32:
		*value = *(int32_t*)(*pos + data_size_type());
		*pos += data_size32();
		break;
	case DATA_INT64:
		*value = *(int64_t*)(*pos + data_size_type());
		*pos += data_size64();
		break;
	default:
		data_error(*pos, DATA_INTV0);
		break;
	}
}

always_inline hot static inline int64_t
unpack_int_at(uint8_t* pos)
{
	int64_t value;
	unpack_int(&pos, &value);
	return value;
}

// string
always_inline hot static inline void
pack_raw(uint8_t** pos, const char* value, uint32_t size)
{
	uint8_t* data = *pos;
	if (size <= 31)
	{
		*data = DATA_STRINGV0 + size;
		*pos += data_size_type();
	} else
	if (size <= INT8_MAX)
	{
		*data = DATA_STRING8;
		*(int8_t*)(data + data_size_type()) = size;
		*pos += data_size8();
	} else
	if (size <= INT16_MAX)
	{
		*data = DATA_STRING16;
		*(int16_t*)(data + data_size_type()) = size;
		*pos += data_size16();
	} else
	{
		*data = DATA_STRING32;
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
pack_string(uint8_t** pos, Str* string)
{
	pack_raw(pos, str_of(string), str_size(string));
}

always_inline hot static inline void
pack_string_cat(uint8_t** pos, char* a, char* b, int a_size, int b_size)
{
	uint8_t* data = *pos;
	uint32_t size = a_size + b_size;
	if (size <= 31)
	{
		*data = DATA_STRINGV0 + size;
		*pos += data_size_type();
	} else
	if (size <= INT8_MAX)
	{
		*data = DATA_STRING8;
		*(int8_t*)(data + data_size_type()) = size;
		*pos += data_size8();
	} else
	if (size <= INT16_MAX)
	{
		*data = DATA_STRING16;
		*(int16_t*)(data + data_size_type()) = size;
		*pos += data_size16();
	} else
	if (size <= INT32_MAX)
	{
		*data = DATA_STRING32;
		*(int32_t*)(data + data_size_type()) = size;
		*pos += data_size32();
	}
	memcpy(*pos, a, a_size);
	*pos += a_size;
	memcpy(*pos, b, b_size);
	*pos += b_size;
}

always_inline hot static inline void
pack_string32(uint8_t** pos, uint32_t size)
{
	uint8_t* data = *pos;
	*data = DATA_STRING32;
	*(int32_t*)(data + data_size_type()) = size;
	*pos += data_size32();
}

always_inline hot static inline void
unpack_raw(uint8_t** pos, char** value, int* size)
{
	switch (**pos) {
	case DATA_STRINGV0 ... DATA_STRINGV31:
		*size = **pos - DATA_STRINGV0;
		*pos += data_size_type();
		break;
	case DATA_STRING8:
		*size = *(int8_t*)(*pos + data_size_type());
		*pos += data_size8();
		break;
	case DATA_STRING16:
		*size = *(int16_t*)(*pos + data_size_type());
		*pos += data_size16();
		break;
	case DATA_STRING32:
		*size = *(int32_t*)(*pos + data_size_type());
		*pos += data_size32();
		break;
	default:
		data_error(*pos, DATA_STRINGV0);
		break;
	}
	*value = (char*)*pos;
	*pos += *size;
}

always_inline hot static inline void
unpack_string(uint8_t** pos, Str* string)
{
	char* value;
	int   value_size;
	unpack_raw(pos, &value, &value_size);
	str_set(string, value, value_size);
}

always_inline hot static inline void
unpack_string_copy(uint8_t** pos, Str* string)
{
	char* value;
	int   value_size;
	unpack_raw(pos, &value, &value_size);
	str_dup(string, value, value_size);
}

// bool
always_inline hot static inline void
pack_bool(uint8_t** pos, bool value)
{
	uint8_t* data = *pos;
	if (value)
		*data = DATA_TRUE;
	else
		*data = DATA_FALSE;
	*pos += data_size_bool();
}

always_inline hot static inline void
unpack_bool(uint8_t** pos, bool* value)
{
	uint8_t* data = *pos;
	if (*data == DATA_TRUE)
		*value = true;
	else
	if (*data == DATA_FALSE)
		*value = false;
	else
		data_error(*pos, DATA_FALSE);
	*pos += data_size_bool();
}

always_inline hot static inline bool
unpack_bool_at(uint8_t* pos)
{
	bool value;
	unpack_bool(&pos, &value);
	return value;
}

// real
always_inline hot static inline void
pack_real(uint8_t** pos, double value)
{
	uint8_t* data = *pos;
	if (value >= FLT_MIN && value <= FLT_MAX)
	{
		*data = DATA_REAL32;
		*(float*)(data + data_size_type()) = value;
		*pos += data_size_type() + sizeof(float);
	} else
	{
		*data = DATA_REAL64;
		*(double*)(data + data_size_type()) = value;
		*pos += data_size_type() + sizeof(double);
	}
}

always_inline hot static inline void
unpack_real(uint8_t** pos, double* value)
{
	uint8_t* data = *pos;
	if (*data == DATA_REAL32)
	{
		*value = *(float*)(data + data_size_type());
		*pos += data_size_type() + sizeof(float);
	} else
	if (*data == DATA_REAL64)
	{
		*value = *(double*)(data + data_size_type());
		*pos += data_size_type() + sizeof(double);
	} else {
		data_error(*pos, DATA_REAL32);
	}
}

always_inline hot static inline double
unpack_real_at(uint8_t* pos)
{
	double value;
	unpack_real(&pos, &value);
	return value;
}

// obj
always_inline hot static inline void
pack_obj(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = DATA_OBJ;
	*pos += data_size_type();
}

always_inline hot static inline void
pack_obj_end(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = DATA_OBJ_END;
	*pos += data_size_type();
}

always_inline hot static inline void
unpack_obj(uint8_t** pos)
{
	if (unlikely(**pos != DATA_OBJ))
		data_error(*pos, DATA_OBJ);
	*pos += data_size_type();
}

always_inline hot static inline bool
unpack_obj_end(uint8_t** pos)
{
	if (unlikely(**pos != DATA_OBJ_END))
		return false;
	*pos += data_size_type();
	return true;
}

// array
always_inline hot static inline void
pack_array(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = DATA_ARRAY;
	*pos += data_size_type();
}

always_inline hot static inline void
pack_array_end(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = DATA_ARRAY_END;
	*pos += data_size_type();
}

always_inline hot static inline void
unpack_array(uint8_t** pos)
{
	if (unlikely(**pos != DATA_ARRAY))
		data_error(*pos, DATA_ARRAY);
	*pos += data_size_type();
}

always_inline hot static inline bool
unpack_array_end(uint8_t** pos)
{
	if (unlikely(**pos != DATA_ARRAY_END))
		return false;
	*pos += data_size_type();
	return true;
}

// null
always_inline hot static inline void
pack_null(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = DATA_NULL;
	*pos += data_size_type();
}

always_inline hot static inline void
unpack_null(uint8_t** pos)
{
	if (unlikely(**pos != DATA_NULL))
		data_error(*pos, DATA_NULL);
	*pos += data_size_type();
}
