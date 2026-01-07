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
	Iov          iov;
	RegionWriter region_writer;
	MetaWriter   meta_writer;
	Codec*       compression;
	Codec*       encryption;
	Source*      source;
	List         link;
};

Writer*
writer_allocate(void);
void writer_free(Writer*);
void writer_reset(Writer*);
void writer_start(Writer*, Source*, File*);
void writer_stop(Writer*, Id*, uint32_t, uint32_t, uint64_t, uint64_t);
void writer_add(Writer*, Row*);

static inline bool
writer_is_limit(Writer* self, uint64_t limit)
{
	return meta_writer_total(&self->meta_writer) >= limit;
}
