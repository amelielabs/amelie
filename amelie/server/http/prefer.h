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

hot static inline bool
prefer_ws(char** pos, char* end)
{
	while (*pos < end && isspace(**pos))
		(*pos)++;
	return *pos == end;
}

hot static inline void
prefer_set(Endpoint* self, Str* name, Str* value)
{
	// timezone / return
	if (str_is_case(name, "timezone", 8))
		opt_string_set(&self->timezone, value);
	else
	if (str_is_case(name, "return", 6))
		opt_string_set(&self->format, value);
}

hot static inline void
prefer_parse(Endpoint* self, Str* header)
{
	// parse Prefer header
	auto pos = header->pos;
	auto end = header->end;

	// [space] <name> [space] [= [space] value] [space] [,]
	for (;;)
	{
		// [space]
		if (prefer_ws(&pos, end))
			goto error;

		// name
		auto start = pos;
		while (pos < end && !isspace(*pos) && *pos != ',' && *pos != '=')
			pos++;
		Str name;
		str_set(&name, start, pos - start);

		Str value;
		str_init(&value);
		if (pos == end)
			break;

		// =
		if (*pos == '=')
		{
			pos++;
			if (unlikely(pos == end))
				goto error;

			// [space]
			if (prefer_ws(&pos, end))
				goto error;

			// value
			start = pos;
			while (pos < end && !isspace(*pos) && *pos != ',')
				pos++;

			str_set(&value, start, pos - start);
		}

		// set option
		prefer_set(self, &name, &value);

		// [space]
		if (prefer_ws(&pos, end))
			break;

		// ,
		if (*pos != ',')	
			goto error;
		pos++;
	}

error:
	// ignore
	return;
}
