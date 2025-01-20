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

typedef enum
{
	TYPE_NULL,
	TYPE_BOOL,
	TYPE_INT,
	TYPE_DOUBLE,
	TYPE_STRING,
	TYPE_JSON,
	TYPE_DATE,
	TYPE_TIMESTAMP,
	TYPE_INTERVAL,
	TYPE_VECTOR,
	TYPE_UUID,
	TYPE_AVG,
	TYPE_STORE,
	TYPE_MAX
} Type;

static inline char*
type_of(Type type)
{
	char* name;
	switch (type) {
	case TYPE_NULL:
		name = "null";
		break;
	case TYPE_BOOL:
		name = "bool";
		break;
	case TYPE_INT:
		name = "int";
		break;
	case TYPE_DOUBLE:
		name = "double";
		break;
	case TYPE_STRING:
		name = "string";
		break;
	case TYPE_JSON:
		name = "json";
		break;
	case TYPE_DATE:
		name = "date";
		break;
	case TYPE_TIMESTAMP:
		name = "timestamp";
		break;
	case TYPE_INTERVAL:
		name = "interval";
		break;
	case TYPE_VECTOR:
		name = "vector";
		break;
	case TYPE_UUID:
		name = "uuid";
		break;
	case TYPE_AVG:
		name = "avg";
		break;
	case TYPE_STORE:
		name = "store";
		break;
	case TYPE_MAX:
		abort();
		break;
	}
	return name;
}

hot static inline int
type_sizeof(Type type)
{
	switch (type) {
	case TYPE_BOOL:
		return sizeof(int8_t);
	case TYPE_DATE:
		return sizeof(int32_t);
	case TYPE_INT:
	case TYPE_TIMESTAMP:
		return sizeof(int64_t);
	case TYPE_DOUBLE:
		return sizeof(double);
	case TYPE_INTERVAL:
		return sizeof(Interval);
	case TYPE_UUID:
		return sizeof(Uuid);
	default:
		// variable
		break;
	}
	return 0;
}

hot static inline int
type_read(Str* name, int* type_size)
{
	*type_size = 0;
	int type = -1;
	if (str_is_case(name, "bool", 4) ||
	    str_is_case(name, "boolean", 7))
	{
		type = TYPE_BOOL;
		*type_size = sizeof(int8_t);
	} else
	if (str_is_case(name, "int8", 4) ||
	    str_is_case(name, "i8", 2)   ||
	    str_is_case(name, "tinyint", 7))
	{
		type = TYPE_INT;
		*type_size = sizeof(int8_t);
	} else
	if (str_is_case(name, "int16", 5) ||
	    str_is_case(name, "i16", 3)   ||
	    str_is_case(name, "smallint", 8))
	{
		type = TYPE_INT;
		*type_size = sizeof(int16_t);
	} else
	if (str_is_case(name, "int", 3)     ||
	    str_is_case(name, "integer", 7) ||
	    str_is_case(name, "int32", 5)   ||
	    str_is_case(name, "i32", 3))
	{
		type = TYPE_INT;
		*type_size = sizeof(int32_t);
	} else
	if (str_is_case(name, "int64", 5)  ||
	    str_is_case(name, "i64", 3)    ||
	    str_is_case(name, "bigint", 6) ||
	    str_is_case(name, "serial", 6))
	{
		type = TYPE_INT;
		*type_size = sizeof(int64_t);
	} else
	if (str_is_case(name, "float", 5) ||
	    str_is_case(name, "f32", 3))
	{
		type = TYPE_DOUBLE;
		*type_size = sizeof(float);
	} else
	if (str_is_case(name, "double", 6) ||
	    str_is_case(name, "f64", 3))
	{
		type = TYPE_DOUBLE;
		*type_size = sizeof(double);
	} else
	if (str_is_case(name, "text", 4) ||
	    str_is_case(name, "string", 6))
	{
		type = TYPE_STRING;
	} else
	if (str_is_case(name, "date", 4))
	{
		type = TYPE_DATE;
		*type_size = sizeof(int32_t);
	} else
	if (str_is_case(name, "json", 4))
	{
		type = TYPE_JSON;
	} else
	if (str_is_case(name, "timestamp", 9))
	{
		type = TYPE_TIMESTAMP;
		*type_size = sizeof(int64_t);
	} else
	if (str_is_case(name, "interval", 8))
	{
		type = TYPE_INTERVAL;
		*type_size = sizeof(Interval);
	} else
	if (str_is_case(name, "vector", 6))
	{
		type = TYPE_VECTOR;
	} else
	if (str_is_case(name, "uuid", 4))
	{
		type = TYPE_UUID;
		*type_size = sizeof(Uuid);
	}
	return type;
}
