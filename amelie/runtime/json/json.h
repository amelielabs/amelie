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

enum
{
	JSON_NULL         = 0,

	JSON_TRUE         = 1,
	JSON_FALSE        = 2,
	JSON_REAL32       = 3,
	JSON_REAL64       = 4,

	JSON_INTV0        = 5,  // reserved values from 0 .. 31
	JSON_INTV31       = 37,
	JSON_INT8         = 38,
	JSON_INT16        = 39,
	JSON_INT32        = 40,
	JSON_INT64        = 41,

	JSON_STRINGV0     = 42, // reserved values from 0 .. 31
	JSON_STRINGV31    = 74,
	JSON_STRING8      = 75,
	JSON_STRING16     = 76,
	JSON_STRING32     = 77,

	JSON_OBJ          = 78,
	JSON_OBJ_END      = 79,
	JSON_ARRAY        = 80,
	JSON_ARRAY_END    = 81
};

static inline char*
json_typeof(int type)
{
	switch (type) {
	case JSON_TRUE:
	case JSON_FALSE:
		return "bool";
	case JSON_NULL:
		return "null";
	case JSON_REAL32:
	case JSON_REAL64:
		return "real";
	case JSON_INTV0 ... JSON_INT64:
		return "int";
	case JSON_STRINGV0 ... JSON_STRING32:
		return "string";
	case JSON_OBJ ... JSON_OBJ_END:
		return "object";
	case JSON_ARRAY ... JSON_ARRAY_END:
		return "array";
	}
	return "<unknown>";
}

always_inline hot static inline void
json_error(uint8_t* data, int type)
{
	error("expected data type '%s', but got '%s'",
	      json_typeof(type),
	      json_typeof(*data));
}

always_inline hot static inline void
json_error_read(void)
{
	error_as(ERROR, "json data read error");
}

always_inline hot static inline int
json_size_type(void)
{
	return sizeof(uint8_t);
}

always_inline hot static inline int
json_size8(void)
{
	return json_size_type() + sizeof(uint8_t);
}

always_inline hot static inline int
json_size16(void)
{
	return json_size_type() + sizeof(uint16_t);
}

always_inline hot static inline int
json_size32(void)
{
	return json_size_type() + sizeof(uint32_t);
}

always_inline hot static inline int
json_size64(void)
{
	return json_size_type() + sizeof(uint64_t);
}

always_inline hot static inline int
json_size_of(uint64_t value)
{
	if (value <= 31)
		return json_size_type();
	if (value <= INT8_MAX)
		return json_size8();
	if (value <= INT16_MAX)
		return json_size16();
	if (value <= INT32_MAX)
		return json_size32();
	return json_size64();
}

//  integer
always_inline hot static inline int
json_size_integer(uint64_t value)
{
	return json_size_of(value);
}

always_inline hot static inline void
json_read_integer(uint8_t** pos, int64_t* value)
{
	switch (**pos) {
	case JSON_INTV0 ... JSON_INTV31:
		*value = **pos - JSON_INTV0;
		*pos += json_size_type();
		break;
	case JSON_INT8:
		*value = *(int8_t*)(*pos + json_size_type());
		*pos += json_size8();
		break;
	case JSON_INT16:
		*value = *(int16_t*)(*pos + json_size_type());
		*pos += json_size16();
		break;
	case JSON_INT32:
		*value = *(int32_t*)(*pos + json_size_type());
		*pos += json_size32();
		break;
	case JSON_INT64:
		*value = *(int64_t*)(*pos + json_size_type());
		*pos += json_size64();
		break;
	default:
		json_error(*pos, JSON_INTV0);
		break;
	}
}

always_inline hot static inline int64_t
json_read_integer_at(uint8_t* pos)
{
	int64_t value;
	json_read_integer(&pos, &value);
	return value;
}

always_inline hot static inline void
json_write_integer(uint8_t** pos, uint64_t value)
{
	uint8_t* data = *pos;
	if (value <= 31)
	{
		*data = JSON_INTV0 + value;
		*pos += json_size_type();
	} else
	if (value <= INT8_MAX)
	{
		*data = JSON_INT8;
		*(int8_t*)(data + json_size_type()) = value;
		*pos += json_size8();
	} else
	if (value <= INT16_MAX)
	{
		*data = JSON_INT16;
		*(int16_t*)(data + json_size_type()) = value;
		*pos += json_size16();
	} else
	if (value <= INT32_MAX)
	{
		*data = JSON_INT32;
		*(int32_t*)(data + json_size_type()) = value;
		*pos += json_size32();
	} else
	{
		*data = JSON_INT64;
		*(int64_t*)(data + json_size_type()) = value;
		*pos += json_size64();
	}
}

always_inline hot static inline bool
json_is_integer(uint8_t* data)
{
	return *data >= JSON_INTV0 && *data <= JSON_INT64;
}

// string
always_inline hot static inline int
json_size_string(int value)
{
	return json_size_of(value) + value;
}

always_inline hot static inline int
json_size_string32(void)
{
	return json_size32();
}

always_inline hot static inline void
json_read_raw(uint8_t** pos, char** value, int* size)
{
	switch (**pos) {
	case JSON_STRINGV0 ... JSON_STRINGV31:
		*size = **pos - JSON_STRINGV0;
		*pos += json_size_type();
		break;
	case JSON_STRING8:
		*size = *(int8_t*)(*pos + json_size_type());
		*pos += json_size8();
		break;
	case JSON_STRING16:
		*size = *(int16_t*)(*pos + json_size_type());
		*pos += json_size16();
		break;
	case JSON_STRING32:
		*size = *(int32_t*)(*pos + json_size_type());
		*pos += json_size32();
		break;
	default:
		json_error(*pos, JSON_STRINGV0);
		break;
	}
	*value = (char*)*pos;
	*pos += *size;
}

always_inline hot static inline void
json_read_string(uint8_t** pos, Str* string)
{
	char* value;
	int   value_size;
	json_read_raw(pos, &value, &value_size);
	str_set(string, value, value_size);
}

always_inline hot static inline void
json_read_string_copy(uint8_t** pos, Str* string)
{
	char* value;
	int   value_size;
	json_read_raw(pos, &value, &value_size);
	str_dup(string, value, value_size);
}

always_inline hot static inline void
json_write_raw(uint8_t** pos, const char* value, uint32_t size)
{
	uint8_t* data = *pos;
	if (size <= 31)
	{
		*data = JSON_STRINGV0 + size;
		*pos += json_size_type();
	} else
	if (size <= INT8_MAX)
	{
		*data = JSON_STRING8;
		*(int8_t*)(data + json_size_type()) = size;
		*pos += json_size8();
	} else
	if (size <= INT16_MAX)
	{
		*data = JSON_STRING16;
		*(int16_t*)(data + json_size_type()) = size;
		*pos += json_size16();
	} else
	{
		*data = JSON_STRING32;
		*(int32_t*)(data + json_size_type()) = size;
		*pos += json_size32();
	}
	if (value && size > 0)
	{
		memcpy(*pos, value, size);
		*pos += size;
	}
}

always_inline hot static inline void
json_write_string(uint8_t** pos, Str* string)
{
	json_write_raw(pos, str_of(string), str_size(string));
}

always_inline hot static inline void
json_write_string_cat(uint8_t** pos, char* a, char* b, int a_size, int b_size)
{
	uint8_t* data = *pos;
	uint32_t size = a_size + b_size;
	if (size <= 31)
	{
		*data = JSON_STRINGV0 + size;
		*pos += json_size_type();
	} else
	if (size <= INT8_MAX)
	{
		*data = JSON_STRING8;
		*(int8_t*)(data + json_size_type()) = size;
		*pos += json_size8();
	} else
	if (size <= INT16_MAX)
	{
		*data = JSON_STRING16;
		*(int16_t*)(data + json_size_type()) = size;
		*pos += json_size16();
	} else
	if (size <= INT32_MAX)
	{
		*data = JSON_STRING32;
		*(int32_t*)(data + json_size_type()) = size;
		*pos += json_size32();
	}
	memcpy(*pos, a, a_size);
	*pos += a_size;
	memcpy(*pos, b, b_size);
	*pos += b_size;
}

always_inline hot static inline void
json_write_string32(uint8_t** pos, uint32_t size)
{
	uint8_t* data = *pos;
	*data = JSON_STRING32;
	*(int32_t*)(data + json_size_type()) = size;
	*pos += json_size32();
}

always_inline hot static inline bool
json_is_string(uint8_t* data)
{
	return *data >= JSON_STRINGV0 && *data <= JSON_STRING32;
}

// bool
always_inline hot static inline int
json_size_bool(void)
{
	return json_size_type();
}

always_inline hot static inline void
json_read_bool(uint8_t** pos, bool* value)
{
	uint8_t* data = *pos;
	if (*data == JSON_TRUE)
		*value = true;
	else
	if (*data == JSON_FALSE)
		*value = false;
	else
		json_error(*pos, JSON_FALSE);
	*pos += json_size_bool();
}

always_inline hot static inline bool
json_read_bool_at(uint8_t* pos)
{
	bool value;
	json_read_bool(&pos, &value);
	return value;
}

always_inline hot static inline void
json_write_bool(uint8_t** pos, bool value)
{
	uint8_t* data = *pos;
	if (value)
		*data = JSON_TRUE;
	else
		*data = JSON_FALSE;
	*pos += json_size_bool();
}

always_inline hot static inline bool
json_is_bool(uint8_t* data)
{
	return *data == JSON_TRUE || *data == JSON_FALSE;
}

// real
always_inline hot static inline int
json_size_real(double value)
{
	if (value >= FLT_MIN && value <= FLT_MAX)
		return json_size_type() + sizeof(float);
	return json_size_type() + sizeof(double);
}
always_inline hot static inline void
json_read_real(uint8_t** pos, double* value)
{
	uint8_t* data = *pos;
	if (*data == JSON_REAL32)
	{
		*value = *(float*)(data + json_size_type());
		*pos += json_size_type() + sizeof(float);
	} else
	if (*data == JSON_REAL64)
	{
		*value = *(double*)(data + json_size_type());
		*pos += json_size_type() + sizeof(double);
	} else {
		json_error(*pos, JSON_REAL32);
	}
}

always_inline hot static inline double
json_read_real_at(uint8_t* pos)
{
	double value;
	json_read_real(&pos, &value);
	return value;
}

always_inline hot static inline void
json_write_real(uint8_t** pos, double value)
{
	uint8_t* data = *pos;
	if (value >= FLT_MIN && value <= FLT_MAX)
	{
		*data = JSON_REAL32;
		*(float*)(data + json_size_type()) = value;
		*pos += json_size_type() + sizeof(float);
	} else
	{
		*data = JSON_REAL64;
		*(double*)(data + json_size_type()) = value;
		*pos += json_size_type() + sizeof(double);
	}
}

always_inline hot static inline bool
json_is_real(uint8_t* data)
{
	return *data == JSON_REAL32 || *data == JSON_REAL64;
}

// obj
always_inline hot static inline int
json_size_obj(void)
{
	return json_size_type();
}

always_inline hot static inline int
json_size_obj_end(void)
{
	return json_size_type();
}

always_inline hot static inline void
json_read_obj(uint8_t** pos)
{
	if (unlikely(**pos != JSON_OBJ))
		json_error(*pos, JSON_OBJ);
	*pos += json_size_type();
}

always_inline hot static inline bool
json_read_obj_end(uint8_t** pos)
{
	if (unlikely(**pos != JSON_OBJ_END))
		return false;
	*pos += json_size_type();
	return true;
}

always_inline hot static inline void
json_write_obj(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = JSON_OBJ;
	*pos += json_size_type();
}

always_inline hot static inline void
json_write_obj_end(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = JSON_OBJ_END;
	*pos += json_size_type();
}

always_inline hot static inline bool
json_is_obj(uint8_t* data)
{
	return *data == JSON_OBJ;
}

always_inline hot static inline bool
json_is_obj_end(uint8_t* data)
{
	return *data == JSON_OBJ_END;
}

// array
always_inline hot static inline int
json_size_array(void)
{
	return json_size_type();
}

always_inline hot static inline int
json_size_array_end(void)
{
	return json_size_type();
}

always_inline hot static inline void
json_read_array(uint8_t** pos)
{
	if (unlikely(**pos != JSON_ARRAY))
		json_error(*pos, JSON_ARRAY);
	*pos += json_size_type();
}

always_inline hot static inline bool
json_read_array_end(uint8_t** pos)
{
	if (unlikely(**pos != JSON_ARRAY_END))
		return false;
	*pos += json_size_type();
	return true;
}

always_inline hot static inline void
json_write_array(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = JSON_ARRAY;
	*pos += json_size_type();
}

always_inline hot static inline void
json_write_array_end(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = JSON_ARRAY_END;
	*pos += json_size_type();
}

always_inline hot static inline bool
json_is_array(uint8_t* data)
{
	return *data == JSON_ARRAY;
}

always_inline hot static inline bool
json_is_array_end(uint8_t* data)
{
	return *data == JSON_ARRAY_END;
}

// null
always_inline hot static inline int
json_size_null(void)
{
	return json_size_type();
}

always_inline hot static inline void
json_read_null(uint8_t** pos)
{
	if (unlikely(**pos != JSON_NULL))
		json_error(*pos, JSON_NULL);
	*pos += json_size_type();
}

always_inline hot static inline void
json_write_null(uint8_t** pos)
{
	uint8_t* data = *pos;
	*data = JSON_NULL;
	*pos += json_size_type();
}

always_inline hot static inline bool
json_is_null(uint8_t* data)
{
	return *data == JSON_NULL;
}
