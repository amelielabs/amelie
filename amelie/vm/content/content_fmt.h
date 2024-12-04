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

typedef struct ContentFmt ContentFmt;

struct ContentFmt
{
	char* pos;
	char* pos_end;
	Str   name;
	bool  opt_array;
	bool  opt_obj;
	bool  opt_pretty;
	Str*  spec;
};

static inline void
content_fmt_init(ContentFmt* self)
{
	self->pos        = NULL;
	self->pos_end    = NULL;
	self->spec       = NULL;
	self->opt_array  = false;
	self->opt_obj    = false;
	self->opt_pretty = false;
	str_init(&self->name);
}

static inline bool
content_fmt_next(ContentFmt* self, Str* name)
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
content_fmt_read(ContentFmt* self, Str* spec)
{
	self->opt_array  = false;
	self->opt_obj    = false;
	self->opt_pretty = false;
	self->pos        = spec->pos;
	self->pos_end    = spec->end;
	self->spec       = spec;
	str_init(&self->name);

	// format_name[-opt ...]
	if (! content_fmt_next(self, &self->name))
		error("invalid format string");

	// options
	Str name;
	while (content_fmt_next(self, &name))
	{
		if (str_is_case(&name, "array", 5)) {
			self->opt_array = true;
		} else
		if (str_is_case(&name, "obj", 3)) {
			self->opt_obj = true;
		} else
		if (str_is_case(&name, "pretty", 6)) {
			self->opt_pretty = true;
		} else {
			error("unrecognized format flag '%.*s'", str_size(&name),
			       str_of(&name));
		}
	}
}
