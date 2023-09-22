#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef enum
{
	ACCESS_UNDEF,
	ACCESS_CLIENT,
	ACCESS_BACKUP,
	ACCESS_REPLICA
} Access;

typedef enum
{
	ACCESS_MODE_ANY,
	ACCESS_MODE_RW,
	ACCESS_MODE_RO
} AccessMode;

static inline Access
access_of(Str* string)
{
	if (str_compare_raw(string, "client", 6))
		return ACCESS_CLIENT;
	if (str_compare_raw(string, "backup", 6))
		return ACCESS_BACKUP;
	if (str_compare_raw(string, "replica", 7))
		return ACCESS_REPLICA;
	return ACCESS_UNDEF;
}

static inline void
access_str(Access self, Str* string)
{
	char* name = "undef";
	int   name_size = 5;
	switch (self) {
	case ACCESS_CLIENT:
		name = "client";
		name_size = 6;
		break;
	case ACCESS_BACKUP:
		name = "backup";
		name_size = 6;
		break;
	case ACCESS_REPLICA:
		name = "replica";
		name_size = 7;
		break;
	default:
		break;
	}
	str_set(string, name, name_size);
}

static inline AccessMode
access_mode_of(Str* string)
{
	if (str_compare_raw(string, "any", 3))
		return ACCESS_MODE_ANY;
	if (str_compare_raw(string, "rw", 2))
		return ACCESS_MODE_RW;
	if (str_compare_raw(string, "ro", 2))
		return ACCESS_MODE_RO;
	return ACCESS_MODE_ANY;
}

static inline void
access_mode_str(AccessMode self, Str* string)
{
	char* name = "any";
	int   name_size = 3;
	switch (self) {
	case ACCESS_MODE_ANY:
		name = "any";
		name_size = 3;
		break;
	case ACCESS_MODE_RW:
		name = "rw";
		name_size = 2;
		break;
	case ACCESS_MODE_RO:
		name = "ro";
		name_size = 2;
		break;
	}
	str_set(string, name, name_size);
}
