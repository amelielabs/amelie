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

typedef struct OutputFmt OutputFmt;

struct OutputFmt
{
	char* pos;
	char* pos_end;
	bool  opt_pretty;
	bool  opt_minimal;
};

static inline void
output_fmt_init(OutputFmt* self)
{
	self->pos         = NULL;
	self->pos_end     = NULL;
	self->opt_pretty  = true;
	self->opt_minimal = true;
}

static inline bool
output_fmt_next(OutputFmt* self, Str* name)
{
	// name[-name ...]
	if (self->pos == self->pos_end)	
		return false;

	auto start = self->pos;
	while (self->pos < self->pos_end)
	{
		if (*self->pos == '-')
			break;
		self->pos++;
	}
	str_set(name, start, self->pos - start);
	if (unlikely(str_empty(name)))
		error("invalid format string");

	// -
	if (self->pos != self->pos_end)
		self->pos++;

	return true;
}

static inline void
output_fmt_read(OutputFmt* self, Str* spec)
{
	self->opt_minimal = true;
	self->opt_pretty  = true;
	self->pos         = spec->pos;
	self->pos_end     = spec->end;

	// name[-name ...]
	Str name;
	while (output_fmt_next(self, &name))
	{
		if (str_is_case(&name, "minimal", 7)) {
			self->opt_minimal = true;
		} else
		if (str_is_case(&name, "full", 4)) {
			self->opt_minimal = false;
		} else
		if (str_is_case(&name, "pretty", 6)) {
			self->opt_pretty = true;
		} else {
			error("unrecognized format option '%.*s'", str_size(&name),
			      str_of(&name));
		}
	}
}
