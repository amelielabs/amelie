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
	DATA_NULL         = 0,

	DATA_TRUE         = 1,
	DATA_FALSE        = 2,
	DATA_REAL32       = 3,
	DATA_REAL64       = 4,

	DATA_INTV0        = 5,  // reserved values from 0 .. 31
	DATA_INTV31       = 37,
	DATA_INT8         = 38,
	DATA_INT16        = 39,
	DATA_INT32        = 40,
	DATA_INT64        = 41,

	DATA_STRINGV0     = 42, // reserved values from 0 .. 31
	DATA_STRINGV31    = 74,
	DATA_STRING8      = 75,
	DATA_STRING16     = 76,
	DATA_STRING32     = 77,

	DATA_OBJ          = 78,
	DATA_OBJ_END      = 79,
	DATA_ARRAY        = 80,
	DATA_ARRAY_END    = 81
};

static inline char*
data_typeof(int type)
{
	switch (type) {
	case DATA_TRUE:
	case DATA_FALSE:
		return "bool";
	case DATA_NULL:
		return "null";
	case DATA_REAL32:
	case DATA_REAL64:
		return "real";
	case DATA_INTV0 ... DATA_INT64:
		return "int";
	case DATA_STRINGV0 ... DATA_STRING32:
		return "string";
	case DATA_OBJ ... DATA_OBJ_END:
		return "object";
	case DATA_ARRAY ... DATA_ARRAY_END:
		return "array";
	}
	return "<unknown>";
}

always_inline hot static inline void
data_error(uint8_t* data, int type)
{
	error("expected data type '%s', but got '%s'",
	      data_typeof(type),
	      data_typeof(*data));
}

always_inline hot static inline void
data_error_read(void)
{
	error_as(ERROR, "json data read error");
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

always_inline hot static inline int
data_size_int(uint64_t value)
{
	return data_size_of(value);
}

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

always_inline hot static inline int
data_size_bool(void)
{
	return data_size_type();
}

always_inline hot static inline int
data_size_real(double value)
{
	if (value >= FLT_MIN && value <= FLT_MAX)
		return data_size_type() + sizeof(float);
	return data_size_type() + sizeof(double);
}

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

always_inline hot static inline int
data_size_null(void)
{
	return data_size_type();
}

always_inline hot static inline bool
data_is_int(uint8_t* data)
{
	return *data >= DATA_INTV0 && *data <= DATA_INT64;
}

always_inline hot static inline bool
data_is_string(uint8_t* data)
{
	return *data >= DATA_STRINGV0 && *data <= DATA_STRING32;
}

always_inline hot static inline bool
data_is_bool(uint8_t* data)
{
	return *data == DATA_TRUE || *data == DATA_FALSE;
}

always_inline hot static inline bool
data_is_real(uint8_t* data)
{
	return *data == DATA_REAL32 || *data == DATA_REAL64;
}

always_inline hot static inline bool
data_is_obj(uint8_t* data)
{
	return *data == DATA_OBJ;
}

always_inline hot static inline bool
data_is_obj_end(uint8_t* data)
{
	return *data == DATA_OBJ_END;
}

always_inline hot static inline bool
data_is_array(uint8_t* data)
{
	return *data == DATA_ARRAY;
}

always_inline hot static inline bool
data_is_array_end(uint8_t* data)
{
	return *data == DATA_ARRAY_END;
}

always_inline hot static inline bool
data_is_null(uint8_t* data)
{
	return *data == DATA_NULL;
}
