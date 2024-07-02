#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

enum
{
	SO_NULL       = 0,

	SO_TRUE       = 1,
	SO_FALSE      = 2,
	SO_REAL32     = 3,
	SO_REAL64     = 4,

	SO_INTV0      = 5,   // reserved values from 0 .. 31
	SO_INTV31     = 37,
	SO_INT8       = 38,
	SO_INT16      = 39,
	SO_INT32      = 40,
	SO_INT64      = 41,

	SO_STRINGV0   = 42, // reserved values from 0 .. 31
	SO_STRINGV31  = 74,
	SO_STRING8    = 75,
	SO_STRING16   = 76,
	SO_STRING32   = 77,

	SO_MAP        = 78,
	SO_MAP_END    = 79,
	SO_ARRAY      = 80,
	SO_ARRAY_END  = 81,

	SO_INTERVAL   = 82
};

static inline char*
type_to_string(int type)
{
	switch (type) {
	case SO_TRUE:
	case SO_FALSE:
		return "bool";
	case SO_NULL:
		return "null";
	case SO_REAL32:
	case SO_REAL64:
		return "real";
	case SO_INTV0 ... SO_INT64:
		return "int";
	case SO_STRINGV0 ... SO_STRING32:
		return "string";
	case SO_MAP ... SO_MAP_END:
		return "map";
	case SO_ARRAY ... SO_ARRAY_END:
		return "array";
	case SO_INTERVAL:
		return "interval";
	}
	return "<unknown>";
}
