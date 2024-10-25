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

static inline int
arg_parse(char* arg, Str* name, Str* value)
{
	// --<option>=<value>
	// --<option>=
	// --<option>
	auto pos = arg;
	if (strncmp(pos, "--", 2) != 0)
		return -1;
	pos += 2;
	auto start = pos;
	while (*pos && *pos != '=')
		pos++;

	str_set(name, start, pos - start);
	if (str_empty(name))
		return -1;

	// value (can be empty)
	if (! *pos) {
		str_init(value);
	} else
	{
		pos++;
		str_set_cstr(value, pos);
	}
	return 0;
}
