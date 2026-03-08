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

typedef struct Collection Collection;

struct Collection
{
	Buf      buf;
	uint64_t size;
	int      count;
};

static inline void
collection_init(Collection* self)
{
	self->size  = 0;
	self->count = 0;
	buf_init(&self->buf);
}

static inline void
collection_free(Collection* self)
{
	buf_free(&self->buf);
}

static inline void
collection_reset(Collection* self)
{
	self->size  = 0;
	self->count = 0;
	buf_reset(&self->buf);
}

static inline bool
collection_empty(Collection* self)
{
	return !self->count;
}

static inline void
collection_add(Collection* self, Row* row)
{
	self->size += row_size(row);
	self->count++;
	buf_write(&self->buf, &row, sizeof(Row**));
}

hot static int
collection_cmp(const void* p1, const void* p2, void* arg)
{
	return compare(arg, *(Row**)p1, *(Row**)p2);
}

static inline void
collection_sort(Collection* self, Keys* keys)
{
	qsort_r(self->buf.start, self->count, sizeof(Row*),
	        collection_cmp, keys);
}
