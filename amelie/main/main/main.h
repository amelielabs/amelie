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

typedef struct Main Main;

struct Main
{
	Console   console;
	bool      home;
	Bookmarks bookmarks;
	Endpoint  endpoint;
	int       argc;
	char**    argv;
};

static inline void
main_advance(Main* self, int advance)
{
	self->argc -= advance;
	self->argv += advance;
	assert(self->argc >= 0);
}

void main_init(Main*, int, char**);
void main_configure(Main*);
void main_free(Main*);
void main_runtime(void*, int, char**);
