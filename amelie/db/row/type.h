#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

enum
{
	TYPE_MAP,
	TYPE_ARRAY,
	TYPE_INT,
	TYPE_BOOL,
	TYPE_REAL,
	TYPE_STRING,
	TYPE_TIMESTAMP,
	TYPE_INTERVAL,
	TYPE_VECTOR
};

hot static inline bool
type_validate(int type, uint8_t* data)
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
	}
	return false;
}

hot static inline const char*
type_of(int type)
{
	switch (type) {
	case TYPE_MAP:       return "map";
	case TYPE_ARRAY:     return "array";
	case TYPE_INT:       return "int";
	case TYPE_BOOL:      return "bool";
	case TYPE_REAL:      return "real";
	case TYPE_STRING:    return "string";
	case TYPE_TIMESTAMP: return "timestamp";
	case TYPE_INTERVAL:  return "interval";
	case TYPE_VECTOR:    return "vector";
	}
	return NULL;
}
