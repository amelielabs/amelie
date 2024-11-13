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
	TYPE_BOOL,
	TYPE_INT8,
	TYPE_INT16,
	TYPE_INT32,
	TYPE_INT64,
	TYPE_FLOAT,
	TYPE_DOUBLE,
	TYPE_TIMESTAMP,
	TYPE_INTERVAL,
	TYPE_TEXT,
	TYPE_JSON,
	TYPE_VECTOR,
	TYPE_MAX
};

hot static inline const char*
type_of(int type)
{
	switch (type) {
	case TYPE_BOOL:      return "bool";
	case TYPE_INT8:      return "int8";
	case TYPE_INT16:     return "int16";
	case TYPE_INT32:     return "int32";
	case TYPE_INT64:     return "int64";
	case TYPE_FLOAT:     return "float";
	case TYPE_DOUBLE:    return "double";
	case TYPE_TIMESTAMP: return "timestamp";
	case TYPE_INTERVAL:  return "interval";
	case TYPE_TEXT:      return "text";
	case TYPE_JSON:      return "json";
	case TYPE_VECTOR:    return "vector";
	}
	return "";
}

hot static inline int
type_read(Str* name)
{
	int type = -1;
	if (str_is(name, "bool", 4) ||
	    str_is(name, "boolean", 7))
	{
		type = TYPE_BOOL;
	} else
	if (str_is(name, "int8", 4) ||
	    str_is(name, "i8", 2))
	{
		type = TYPE_INT8;
	} else
	if (str_is(name, "int16", 5) ||
	    str_is(name, "i16", 3))
	{
		type = TYPE_INT16;
	} else
	if (str_is(name, "int", 3) ||
	    str_is(name, "integer", 7) ||
	    str_is(name, "int32", 5) ||
	    str_is(name, "i32", 3))
	{
		type = TYPE_INT32;
	} else
	if (str_is(name, "int64", 5) ||
	    str_is(name, "i64", 3))
	{
		type = TYPE_INT64;
	} else
	if (str_is(name, "float", 5))
	{
		type = TYPE_FLOAT;
	} else
	if (str_is(name, "double", 6))
	{
		type = TYPE_DOUBLE;
	} else
	if (str_is(name, "timestamp", 9))
	{
		type = TYPE_TIMESTAMP;
	} else
	if (str_is(name, "interval", 8))
	{
		type = TYPE_INTERVAL;
	} else
	if (str_is(name, "text", 4) ||
	    str_is(name, "string", 6))
	{
		type = TYPE_TEXT;
	} else
	if (str_is(name, "json", 4))
	{
		type = TYPE_JSON;
	} else
	if (str_is(name, "vector", 6))
	{
		type = TYPE_VECTOR;
	}
	return type;
}

hot static inline int
type_size(int type)
{
	switch (type) {
	// fixed types
	case TYPE_BOOL:
	case TYPE_INT8:
		return sizeof(int8_t);
	case TYPE_INT16:
		return sizeof(int16_t);
	case TYPE_INT32:
		return sizeof(int32_t);
	case TYPE_INT64:
		return sizeof(int64_t);
	case TYPE_FLOAT:
		return sizeof(float);
	case TYPE_DOUBLE:
		return sizeof(double);
	case TYPE_TIMESTAMP:
		return sizeof(int64_t);
	case TYPE_INTERVAL:
		return sizeof(Interval);
	// variable types
	case TYPE_TEXT:
	case TYPE_JSON:
	case TYPE_VECTOR:
		break;
	}
	return 0;
}
