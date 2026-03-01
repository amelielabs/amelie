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

typedef struct Writer Writer;

struct Writer
{
	File*        file;
	int          region_size;
	RegionWriter region_writer;
	MetaWriter   meta_writer;
	Storage*     storage;
	Writer*      next;
};

Writer*
writer_allocate(void);
void writer_free(Writer*);
void writer_reset(Writer*);
void writer_start(Writer*, File*, Storage*, int);
void writer_stop(Writer*);
void writer_add(Writer*, Row*);

static inline bool
writer_is_limit(Writer* self, uint64_t limit)
{
	return meta_writer_total(&self->meta_writer) >= limit;
}
