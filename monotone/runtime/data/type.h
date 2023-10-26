#pragma once

//
// monotone
//
// SQL OLTP database
//

enum
{
	MN_NULL       = 0,
	MN_TRUE       = 1,
	MN_FALSE      = 2,
	MN_REAL32     = 3,
	MN_REAL64     = 4,

	MN_INTV0      = 5,   // reserved values from 0 .. 31
	MN_INTV31     = 37,
	MN_INT8       = 38,
	MN_INT16      = 39,
	MN_INT32      = 40,
	MN_INT64      = 41,

	MN_ARRAYV0    = 42,  // reserved values from 0 .. 31
	MN_ARRAYV31   = 74,
	MN_ARRAY8     = 75,
	MN_ARRAY16    = 76,
	MN_ARRAY32    = 77,

	MN_MAPV0      = 78,  // reserved values from 0 .. 31
	MN_MAPV31     = 110,
	MN_MAP8       = 111,
	MN_MAP16      = 112,
	MN_MAP32      = 113,

	MN_STRINGV0   = 114, // reserved values from 0 .. 31
	MN_STRINGV31  = 146,
	MN_STRING8    = 147,
	MN_STRING16   = 148,
	MN_STRING32   = 149,

	MN_PARTIAL    = 150, // reserved values for partials
	MN_PARTIALMAX = 200
};

static inline char*
type_to_string(int type)
{
	switch (type) {
	case MN_TRUE:
	case MN_FALSE:
		return "bool";
	case MN_NULL:
		return "null";
	case MN_REAL32:
	case MN_REAL64:
		return "real";
	case MN_INTV0 ... MN_INT64:
		return "int";
	case MN_ARRAYV0 ... MN_ARRAY32:
		return "array";
	case MN_MAPV0 ... MN_MAP32:
		return "map";
	case MN_STRINGV0 ... MN_STRING32:
		return "string";
	case MN_PARTIAL ... MN_PARTIALMAX:
		return "partial";
	}
	return "<unknown>";
}
