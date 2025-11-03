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
	Str  header;
	int  opts_count;
	Buf  opts;
	Str* opt_timezone;
	Str* opt_return;
};

static inline void
prefer_init(Prefer* self)
{
	self->opt_timezone = NULL;
	self->opt_return   = NULL;
	self->opts_count   = 0;
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
	self->opt_timezone = NULL;
	self->opt_return   = NULL;
	self->opts_count   = 0;
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

hot static inline void
prefer_process(Prefer* self)
{
	// configure session preferences
	auto opt = (PreferOpt*)self->opts.start;
	auto end = (PreferOpt*)self->opts.position;
	for (; opt < end; opt++)
	{
		// timezone
		if (str_is_case(&opt->name, "timezone", 8))
		{
			self->opt_timezone = &opt->value;
			continue;
		}

		// return
		if (str_is_case(&opt->name, "return", 6))
		{
			self->opt_return = &opt->value;
			continue;
		}
	}
}
