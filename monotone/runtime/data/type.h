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
	MN_FLOAT      = 3,

	MN_INTV0      = 4,   // reserved values from 0 .. 31
	MN_INTV31     = 36,
	MN_INT8       = 37,
	MN_INT16      = 38,
	MN_INT32      = 39,
	MN_INT64      = 40,

	MN_ARRAYV0    = 41,  // reserved values from 0 .. 31
	MN_ARRAYV31   = 73,
	MN_ARRAY8     = 74,
	MN_ARRAY16    = 75,
	MN_ARRAY32    = 76,

	MN_MAPV0      = 77,  // reserved values from 0 .. 31
	MN_MAPV31     = 109,
	MN_MAP8       = 110,
	MN_MAP16      = 111,
	MN_MAP32      = 112,

	MN_STRINGV0   = 113, // reserved values from 0 .. 31
	MN_STRINGV31  = 145,
	MN_STRING8    = 146,
	MN_STRING16   = 147,
	MN_STRING32   = 148,

	MN_PARTIAL    = 149, // reserved values for partials
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
	case MN_FLOAT:
		return "float";
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
