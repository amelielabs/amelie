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

typedef struct Encoding Encoding;

struct Encoding
{
	Str* compression;
	int  compression_level;
	Str* encryption;
	Str* encryption_key;
};

static inline void
encoding_init(Encoding* self)
{
	self->compression       = NULL;
	self->compression_level = 0;
	self->encryption        = 0;
	self->encryption_key    = 0;
}
