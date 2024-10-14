#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

enum
{
	AM_NULL       = 0,

	AM_TRUE       = 1,
	AM_FALSE      = 2,
	AM_REAL32     = 3,
	AM_REAL64     = 4,

	AM_INTV0      = 5,  // reserved values from 0 .. 31
	AM_INTV31     = 37,
	AM_INT8       = 38,
	AM_INT16      = 39,
	AM_INT32      = 40,
	AM_INT64      = 41,

	AM_STRINGV0   = 42, // reserved values from 0 .. 31
	AM_STRINGV31  = 74,
	AM_STRING8    = 75,
	AM_STRING16   = 76,
	AM_STRING32   = 77,

	AM_OBJ        = 78,
	AM_OBJ_END    = 79,
	AM_ARRAY      = 80,
	AM_ARRAY_END  = 81,

	AM_INTERVAL   = 82,
	AM_TIMESTAMP  = 83,
	AM_VECTOR     = 84
};

static inline char*
type_to_string(int type)
{
	switch (type) {
	case AM_TRUE:
	case AM_FALSE:
		return "bool";
	case AM_NULL:
		return "null";
	case AM_REAL32:
	case AM_REAL64:
		return "real";
	case AM_INTV0 ... AM_INT64:
		return "int";
	case AM_STRINGV0 ... AM_STRING32:
		return "string";
	case AM_OBJ... AM_OBJ_END:
		return "object";
	case AM_ARRAY ... AM_ARRAY_END:
		return "array";
	case AM_INTERVAL:
		return "interval";
	case AM_TIMESTAMP:
		return "timestamp";
	case AM_VECTOR:
		return "vector";
	}
	return "<unknown>";
}
