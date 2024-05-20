#pragma once

//
// sonata.
//
// SQL Database for JSON.
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

	SO_ARRAYV0    = 42,  // reserved values from 0 .. 31
	SO_ARRAYV31   = 74,
	SO_ARRAY8     = 75,
	SO_ARRAY16    = 76,
	SO_ARRAY32    = 77,

	SO_MAPV0      = 78,  // reserved values from 0 .. 31
	SO_MAPV31     = 110,
	SO_MAP8       = 111,
	SO_MAP16      = 112,
	SO_MAP32      = 113,

	SO_STRINGV0   = 114, // reserved values from 0 .. 31
	SO_STRINGV31  = 146,
	SO_STRING8    = 147,
	SO_STRING16   = 148,
	SO_STRING32   = 149
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
	case SO_ARRAYV0 ... SO_ARRAY32:
		return "array";
	case SO_MAPV0 ... SO_MAP32:
		return "map";
	case SO_STRINGV0 ... SO_STRING32:
		return "string";
	}
	return "<unknown>";
}
