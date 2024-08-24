#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline int
arg_parse(char* arg, Str* name, Str* value)
{
	// --<option>=<value>
	// --<option>=
	auto pos = arg;
	if (strncmp(pos, "--", 2) != 0)
		return -1;
	pos += 2;
	auto start = pos;
	while (*pos && *pos != '=')
		pos++;
	if (*pos != '=')
		return -1;
	str_set(name, start, pos - start);
	if (str_empty(name))
		return -1;
	pos++;
	// value (can be empty)
	str_set_cstr(value, pos);
	return 0;
}
