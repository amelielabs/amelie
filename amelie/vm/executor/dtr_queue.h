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

typedef struct DtrQueue DtrQueue;

struct DtrQueue
{
	Buf list;
	int list_count;
};

static inline Dtr*
dtr_queue_at(DtrQueue* self, int order)
{
	return ((Dtr**)self->list.start)[order];
}

static inline void
dtr_queue_init(DtrQueue* self)
{
	self->list_count = 0;
	buf_init(&self->list);
}

static inline void
dtr_queue_free(DtrQueue* self)
{
	buf_free(&self->list);
}

static inline void
dtr_queue_reset(DtrQueue* self)
{
	self->list_count = 0;
	buf_reset(&self->list);
}

static inline bool
dtr_queue_empty(DtrQueue* self)
{
	return !self->list_count;
}

static inline void
dtr_queue_add(DtrQueue* self, Dtr* dtr)
{
	buf_write(&self->list, &dtr, sizeof(Dtr**));
	self->list_count++;
}

static inline int
dtr_queue_sort_cb(const void* a, const void* b)
{
	return compare_uint64(((Dtr*)a)->tsn, ((Dtr*)b)->tsn);
}

static inline void
dtr_queue_sort(DtrQueue* self)
{
	// order dtrs by tsn
	if (self->list_count <= 1)
		return;
	qsort(self->list.start, self->list_count, sizeof(Dtr*),
	      dtr_queue_sort_cb);
}
