#pragma once

//
// monotone
//
// SQL OLTP database
//

enum
{
	TYPE_MAP,
	TYPE_ARRAY,
	TYPE_INT,
	TYPE_BOOL,
	TYPE_FLOAT,
	TYPE_STRING
};

hot static inline bool
schema_type_validate(int type, uint8_t* data)
{
	switch (type) {
	case TYPE_MAP:
		return data_is_map(data);
	case TYPE_ARRAY:
		return data_is_array(data);
	case TYPE_INT:
		return data_is_integer(data);
	case TYPE_BOOL:
		return data_is_bool(data);
	case TYPE_FLOAT:
		return data_is_float(data);
	case TYPE_STRING:
		return data_is_string(data);
	}
	return false;
}

hot static inline const char*
schema_type_of(int type)
{
	switch (type) {
	case TYPE_MAP:    return "map";
	case TYPE_ARRAY:  return "array";
	case TYPE_INT:    return "int";
	case TYPE_BOOL:   return "bool";
	case TYPE_FLOAT:  return "float";
	case TYPE_STRING: return "string";
	}
	return NULL;
}
