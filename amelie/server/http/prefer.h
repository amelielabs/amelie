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

typedef struct PreferOpt PreferOpt;
typedef struct Prefer    Prefer;

struct PreferOpt
{
	Str name;
	Str value;
};

struct Prefer
{
	Str header;
	int opts_count;
	Buf opts;
};

static inline void
prefer_init(Prefer* self)
{
	self->opts_count = 0;
	buf_init(&self->opts);
	str_init(&self->header);
}

static inline void
prefer_free(Prefer* self)
{
	buf_free(&self->opts);
}

static inline void
prefer_reset(Prefer* self)
{
	self->opts_count = 0;
	buf_reset(&self->opts);
	str_init(&self->header);
}

static inline bool
prefer_ws(char** pos, char* end)
{
	while (*pos < end && isspace(**pos))
		(*pos)++;
	return *pos == end;
}

hot static inline void
prefer_set(Prefer* self, Str* header)
{
	self->header = *header;

	// parse header
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

		self->opts_count++;
		auto opt = (PreferOpt*)buf_claim(&self->opts, sizeof(PreferOpt));
		str_set(&opt->name, start, pos - start);
		str_init(&opt->value);
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
			str_set(&opt->value, start, pos - start);
		}

		// [space]
		if (prefer_ws(&pos, end))
			break;

		// ,
		if (*pos != ',')	
			goto error;
		pos++;
	}
	return;

error:
	prefer_reset(self);
}
