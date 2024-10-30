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
	TYPE_OBJ,
	TYPE_ARRAY,
	TYPE_INT,
	TYPE_BOOL,
	TYPE_REAL,
	TYPE_STRING,
	TYPE_TIMESTAMP,
	TYPE_INTERVAL,
	TYPE_VECTOR,
	TYPE_AGG
};

hot static inline bool
type_validate(int type, uint8_t* data)
{
	switch (type) {
	case TYPE_OBJ:
		return data_is_obj(data);
	case TYPE_ARRAY:
		return data_is_array(data);
	case TYPE_INT:
		return data_is_integer(data);
	case TYPE_BOOL:
		return data_is_bool(data);
	case TYPE_REAL:
		return data_is_real(data);
	case TYPE_STRING:
		return data_is_string(data);
	case TYPE_TIMESTAMP:
		return data_is_timestamp(data);
	case TYPE_INTERVAL:
		return data_is_interval(data);
	case TYPE_VECTOR:
		return data_is_vector(data);
	case TYPE_AGG:
		return data_is_agg(data);
	}
	return false;
}

hot static inline const char*
type_of(int type)
{
	switch (type) {
	case TYPE_OBJ:       return "object";
	case TYPE_ARRAY:     return "array";
	case TYPE_INT:       return "int";
	case TYPE_BOOL:      return "bool";
	case TYPE_REAL:      return "real";
	case TYPE_STRING:    return "string";
	case TYPE_TIMESTAMP: return "timestamp";
	case TYPE_INTERVAL:  return "interval";
	case TYPE_VECTOR:    return "vector";
	case TYPE_AGG:       return "aggregate";
	}
	return "";
}

hot static inline int
type_read(Str* name)
{
	int type = -1;
	if (str_is(name, "int", 3) ||
	    str_is(name, "integer", 7))
	{
		type = TYPE_INT;
	} else
	if (str_is(name, "real", 4))
	{
		type = TYPE_REAL;
	} else
	if (str_is(name, "bool", 4) ||
	    str_is(name, "boolean", 7))
	{
		type = TYPE_BOOL;
	} else
	if (str_is(name, "text", 4) ||
	    str_is(name, "string", 6))
	{
		type = TYPE_STRING;
	} else
	if (str_is(name, "array", 5))
	{
		type = TYPE_ARRAY;
	} else
	if (str_is(name, "object", 6) ||
	    str_is(name, "obj", 3))
	{
		type = TYPE_OBJ;
	} else
	if (str_is(name, "timestamp", 9))
	{
		type = TYPE_TIMESTAMP;
	} else
	if (str_is(name, "interval", 8))
	{
		type = TYPE_INTERVAL;
	} else
	if (str_is(name, "vector", 6))
	{
		type = TYPE_VECTOR;
	} else
	if (str_is(name, "aggregate", 9) ||
	    str_is(name, "agg", 3))
	{
		type = TYPE_AGG;
	}
	return type;
}
